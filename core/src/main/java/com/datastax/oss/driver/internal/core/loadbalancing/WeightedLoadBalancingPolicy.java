/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.loadbalancing;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A load balancing policy that optimally balances between sending load to local token holder,
 * rack replicas, and local datacenter replicas (in that order).
 *
 * The default weights are good for the vast majority of use cases, but you can tweak them to get different behavior.
 */
public class WeightedLoadBalancingPolicy extends DefaultLoadBalancingPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(WeightedLoadBalancingPolicy.class);
  // Each client will randomly skew so traffic is introduced gradually to a newly up replica
  // Each client will start sending at a period between 15s and 30, and they will gradually
  // increase load over the next 15 seconds.
  private static final long DELAY_TRAFFIC_SKEW_MILLIS = SECONDS.toMillis(15);
  private static final long DELAY_TRAFFIC_MILLIS =
      DELAY_TRAFFIC_SKEW_MILLIS + ThreadLocalRandom.current().nextLong(DELAY_TRAFFIC_SKEW_MILLIS);

  // By default we will only score this many nodes, the rest will get added on without scoring.
  // We don't usually need to score every single node if there are more than a few.
  static final int DEFAULT_SCORED_PLAN_SIZE = 8;
  // Default multiplicative weights. Think of this like "How much concurrency must occur
  // before I fail off this node to the next". Note that these defaults are intentionally
  // meant to shed load to unloaded rack coordinators when a replica set is all
  // relatively heavily loaded (specifically 3x as loaded).
  static final double DEFAULT_WEIGHT_NON_RACK = 4.0;
  static final double DEFAULT_WEIGHT_NON_REPLICA = 12.0;
  static final double DEFAULT_WEIGHT_STARTING = 16.0;
  static final double DEFAULT_WEIGHT_UNHEALTHY = 64.0;

  private final int planSize;
  private final double weightNonRack;
  private final double weightNonReplica;
  private final double weightStarting;
  private final double weightUnhealthy;

  public WeightedLoadBalancingPolicy(
      @NonNull DriverContext context,
      @NonNull String profileName) {
    super(context, profileName);
    this.planSize = profile.getInt(DefaultDriverOption.LOAD_BALANCING_SCORED_PLAN_SIZE, DEFAULT_SCORED_PLAN_SIZE);
    // Choices of weights will change how this load balancer prefers endpoints.
    // The weight is relative to the outstanding concurrency.
    this.weightNonRack = profile.getDouble(
        DefaultDriverOption.LOAD_BALANCING_WEIGHT_NON_RACK, DEFAULT_WEIGHT_NON_RACK);
    this.weightNonReplica = profile.getDouble(
        DefaultDriverOption.LOAD_BALANCING_WEIGHT_NON_REPLICA, DEFAULT_WEIGHT_NON_REPLICA);
    this.weightStarting = profile.getDouble(
        DefaultDriverOption.LOAD_BALANCING_WEIGHT_STARTING, DEFAULT_WEIGHT_STARTING);
    this.weightUnhealthy = profile.getDouble(
        DefaultDriverOption.LOAD_BALANCING_WEIGHT_UNHEALTHY, DEFAULT_WEIGHT_UNHEALTHY);
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(Request request, Session session) {
    if (session == null) {
      return super.newQueryPlan(request, null);
    }

    // Take a copy of nodes and reference to replicas since the node map is concurrent
    Set<Node> dcNodeSet = getLiveNodes().dc(getLocalDatacenter());
    Set<Node> replicaSet = getReplicas(request, session);

    long nowNanos = nanoTime();
    long nowMillis = milliTime();

    // collect known replica nodes
    List<NodeScore> nodeScores = new ArrayList<>(this.planSize);
    for (Node replicaNode : replicaSet) {
      if (dcNodeSet.contains(replicaNode)) {
        nodeScores.add(new NodeScore(replicaNode,
                getWeightedScore(replicaNode, session, nowMillis, nowNanos, true)));

        if (nodeScores.size() == this.planSize) {
          break; // TODO (akhaku) add the rest of the nodes once we port the tests to OSS
        }
      }
    }

    // collect any non-replicas, if we need to and there are some available
    if (nodeScores.size() < this.planSize && nodeScores.size() < dcNodeSet.size()) {
      Random rand = getRandom();
      final Node[] dcNodes = dcNodeSet.toArray(new Node[0]);

      for (int i = 0; i < dcNodes.length; i++) {
        // pick a random target; shuffle it up front, so we don't revisit
        int nextIndex = i + rand.nextInt(dcNodes.length - i);
        ArrayUtils.swap(dcNodes, i, nextIndex);
        Node dcNode = dcNodes[i];

        // skip replicas; they were already inserted
        // otherwise, found a valid node: score it
        if (!replicaSet.contains(dcNode)) {
          nodeScores.add(new NodeScore(dcNode,
                  getWeightedScore(dcNode, session, nowMillis, nowNanos, false)));

          // if we scored, we might by now have already scored enough of what we need
          if (nodeScores.size() == this.planSize || nodeScores.size() == dcNodes.length) {
            break;
          }
        }
      }

      // by now we've scored everything we need to meet planSize, or if not, at least everything available
    }

    // At this point we have a small, typically 8 element array containing all local
    // datacenter replicas and potentially some random choices from the rest of the datacenter.
    //
    // We now rank nodes by a score function that takes into account outstanding requests weighted
    // by replica status, rack placement, uptime, and health status. In general, we expect to
    // see the following order
    //   1. Rack replicas
    //   2. Datacenter replicas
    //   3. Rack nodes
    //   4. Datacenter nodes
    // We expect these orderings to move around when nodes are overloaded. For example if the
    // local zone replica has too much load we will failover to other replicas. If those
    // replicas are too slow we will failover to other rack nodes.

    // sort, extract, convert to query plan
    nodeScores.sort(Comparator.comparingDouble(NodeScore::getScore));
    Node[] candidate = new Node[nodeScores.size()];
    for (int i = 0; i < candidate.length; i++) {
      candidate[i] = nodeScores.get(i).getNode();
    }

    QueryPlan queryPlan = candidate.length == 0 ? QueryPlan.EMPTY : new SimpleQueryPlan((Object[]) candidate);
    return maybeAddDcFailover(request, queryPlan);
  }

  protected String getLocalRack() {
    return ""; // TODO (akhaku) internally we passed it through the context, for OSS perhaps something like the local DC helper?
  }

  protected boolean inRack(Node node) {
    if (node == null || node.getRack() == null) return false;
    return node.getRack().equals(this.getLocalRack());
  }

  protected double getWeightedScore(Node node, Session session, long nowMillis, long nowNanos, boolean isReplica) {
    int base = Math.min(32768, 1 + getInFlight(node, session));
    double weight = 1.0;

    if (!inRack(node)) weight *= this.weightNonRack; // 4.0
    if (!isReplica) weight *= this.weightNonReplica; // 12.0
    if (isUnhealthy(node, session, nowNanos)) weight *= this.weightUnhealthy; // 64.0

    // We want to gradually ramp up traffic, shedding heavily at first and then allowing it back
    // in gradually. Note:
    //
    // 1. We cannot use nowNanos for this since node.getUpSinceMillis uses
    //    System.currentTimeMillis (which may have no relation to nano time).
    // 2. getUpSinceMillis might return 0 or -1, in either case don't consider it freshly up.
    // 3. When a client starts up everything will appear to be freshly up, which is fine since
    //    all nodes will randomly be shuffled to the front and back.
    long millisSinceUp = nowMillis - node.getUpSinceMillis();
    if (millisSinceUp < (DELAY_TRAFFIC_MILLIS + DELAY_TRAFFIC_SKEW_MILLIS)) {
      double pShed = 1.0 - ((double) millisSinceUp / (DELAY_TRAFFIC_MILLIS + DELAY_TRAFFIC_SKEW_MILLIS));
      if (pShed > getRandom().nextDouble()) {
        if (LOG.isTraceEnabled()) {
          String shed = String.format("%.2f", pShed);
          LOG.trace("[{}] shedding at startup [pShed={}, millisSinceUp={}]", node, shed, millisSinceUp);
        }
        weight *= this.weightStarting; // 16.0
      }
    }

    double score = base * weight;
    if (LOG.isDebugEnabled()) {
      String msg = String.format("score=%.2f [base=%d, weight=%.2f]", score, base, weight);
      LOG.debug("[{}] {}", node, msg);
    }
    return score;
  }

  protected long milliTime() {
    return System.currentTimeMillis();
  }

  protected Random getRandom() {
    return ThreadLocalRandom.current();
  }

  /**
   * Wraps a Node alongside its score.
   *
   * Calculating scores is expensive, and not stable (could vary). By wrapping them we can be sure the score
   * is calculated only once and does not change during processing.
   */
  static class NodeScore {
    final double score;
    final Node node;

    public NodeScore(Node node, double score) {
      this.node = node;
      this.score = score;
    }

    public Node getNode() {
      return node;
    }

    public double getScore() {
      return score;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NodeScore nodeScore = (NodeScore) o;
      return Double.compare(score, nodeScore.score) == 0 && Objects.equals(node, nodeScore.node);
    }

    @Override
    public int hashCode() {
      return Objects.hash(score, node);
    }
  }
}
