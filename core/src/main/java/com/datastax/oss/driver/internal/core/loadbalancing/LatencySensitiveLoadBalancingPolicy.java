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

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheLoader;
import com.datastax.oss.driver.shaded.guava.common.cache.LoadingCache;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencySensitiveLoadBalancingPolicy extends DefaultLoadBalancingPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(LatencySensitiveLoadBalancingPolicy.class);

  protected final LoadingCache<Node, NodeLatencyTracker> latencies;

  private final long RETRY_PERIOD = TimeUnit.SECONDS.toNanos(2);

  private final long THRESHOLD_TO_ACCOUNT = 100;

  public LatencySensitiveLoadBalancingPolicy(
      @NonNull DriverContext context, @NonNull String profileName) {
    super(context, profileName);
    CacheLoader<Node, NodeLatencyTracker> cacheLoader =
        new CacheLoader<Node, NodeLatencyTracker>() {
          @Override
          public NodeLatencyTracker load(@NonNull Node node) {
            return new NodeLatencyTracker(THRESHOLD_TO_ACCOUNT);
          }
        };
    latencies = CacheBuilder.newBuilder().weakKeys().build(cacheLoader);
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    // Take a snapshot since the set is concurrent:
    Object[] currentNodes = getLiveNodes().dc(getLocalDatacenter()).toArray();

    Set<Node> allReplicas = getReplicas(request, session);
    int replicaCount = 0; // in currentNodes

    if (!allReplicas.isEmpty()) {

      // Move replicas to the beginning of the plan
      for (int i = 0; i < currentNodes.length; i++) {
        Node node = (Node) currentNodes[i];
        if (allReplicas.contains(node)) {
          ArrayUtils.bubbleUp(currentNodes, i, replicaCount);
          replicaCount++;
        }
      }

      if (replicaCount > 1) {

        shuffleHead(currentNodes, replicaCount);

        if (replicaCount > 2) {

          assert session != null;

          Node newestUpReplica = null;

          long mostRecentUpTimeNanos = -1;
          long now = nanoTime();
          for (int i = 0; i < replicaCount; i++) {
            Node node = (Node) currentNodes[i];
            assert node != null;
            Long upTimeNanos = upTimes.get(node);
            if (upTimeNanos != null
                && now - upTimeNanos - NEWLY_UP_INTERVAL_NANOS < 0
                && upTimeNanos - mostRecentUpTimeNanos > 0) {
              newestUpReplica = node;
              mostRecentUpTimeNanos = upTimeNanos;
            }
          }

          // When:
          // - there isn't any newly UP replica
          // - there are 3 replicas in total
          // bubble down the slowest replica to the end of the replicas
          // - else if there are more than 3 replicas in total
          // bubble down the slowest 2 replicas in the first 4 replicas out of the first 2 positions
          if (newestUpReplica == null) {
            if (replicaCount == 3) {
              compareAndSwapSlowReplica(currentNodes, 0, 1);
              compareAndSwapSlowReplica(currentNodes, 1, 2);
              compareAndSwapSlowReplica(currentNodes, 0, 1);
            } else {
              for (int i = 0; i < 3; i++) {
                compareAndSwapSlowReplica(currentNodes, i, i + 1);
              }
              for (int i = 0; i < 2; i++) {
                compareAndSwapSlowReplica(currentNodes, i, i + 1);
              }
            }
          }

          // When:
          // - there is a newly UP replica and
          // - the replica in first or second position is the most recent replica marked as UP and
          // - dice roll 1d4 != 1
          else if ((newestUpReplica == currentNodes[0] || newestUpReplica == currentNodes[1])
              && diceRoll1d4() != 1) {

            // Send it to the back of the replicas
            ArrayUtils.bubbleDown(
                currentNodes, newestUpReplica == currentNodes[0] ? 0 : 1, replicaCount - 1);
          }

          // Reorder the first two replicas in the shuffled list based on the number of
          // in-flight requests
          if (getInFlight((Node) currentNodes[0], session)
              > getInFlight((Node) currentNodes[1], session)) {
            ArrayUtils.swap(currentNodes, 0, 1);
          }
        }
      }
    }

    LOG.trace("[{}] Prioritizing {} local replicas", logPrefix, replicaCount);

    // Round-robin the remaining nodes
    ArrayUtils.rotate(
        currentNodes,
        replicaCount,
        currentNodes.length - replicaCount,
        roundRobinAmount.getAndUpdate(INCREMENT));

    QueryPlan plan = currentNodes.length == 0 ? QueryPlan.EMPTY : new SimpleQueryPlan(currentNodes);
    return maybeAddDcFailover(request, plan);
  }

  @Override
  public void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    latencies.getUnchecked(node).add(latencyNanos);
  }

  @Override
  public void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    latencies.getUnchecked(node).add(latencyNanos);
  }

  protected static class TimestampedAverage {

    private final long timestamp;
    protected final long average;
    private final long nbMeasure;

    TimestampedAverage(long timestamp, long average, long nbMeasure) {
      this.timestamp = timestamp;
      this.average = average;
      this.nbMeasure = nbMeasure;
    }
  }

  protected static class NodeLatencyTracker {

    // just for start up
    private final long thresholdToAccount;
    private final AtomicReference<TimestampedAverage> current =
        new AtomicReference<TimestampedAverage>();

    private final long scale = TimeUnit.MILLISECONDS.toNanos(100);

    NodeLatencyTracker(long thresholdToAccount) {
      this.thresholdToAccount = thresholdToAccount;
    }

    public void add(long newLatencyNanos) {
      TimestampedAverage previous, next;
      do {
        previous = current.get();
        next = computeNextAverage(previous, newLatencyNanos);
      } while (next != null && !current.compareAndSet(previous, next));
    }

    private TimestampedAverage computeNextAverage(
        TimestampedAverage previous, long newLatencyNanos) {

      long currentTimestamp = System.nanoTime();

      long nbMeasure = previous == null ? 1 : previous.nbMeasure + 1;
      if (nbMeasure < thresholdToAccount)
        return new TimestampedAverage(currentTimestamp, -1L, nbMeasure);

      if (previous == null || previous.average < 0)
        return new TimestampedAverage(currentTimestamp, newLatencyNanos, nbMeasure);

      // Note: it's possible for the delay to be 0, in which case newLatencyNanos will basically be
      // discarded. It's fine: nanoTime is precise enough in practice that even if it happens, it
      // will be very rare, and discarding a latency every once in a while is not the end of the
      // world.
      // We do test for negative value, even though in theory that should not happen, because it
      // seems
      // that historically there has been bugs here
      // (https://blogs.oracle.com/dholmes/entry/inside_the_hotspot_vm_clocks)
      // so while this is almost surely not a problem anymore, there's no reason to break the
      // computation
      // if this even happen.
      long delay = currentTimestamp - previous.timestamp;
      if (delay <= 0) return null;

      double scaledDelay = ((double) delay) / scale;
      // Note: We don't use log1p because we it's quite a bit slower and we don't care about the
      // precision (and since we
      // refuse ridiculously big scales, scaledDelay can't be so low that scaledDelay+1 == 1.0 (due
      // to rounding)).
      double prevWeight = Math.log(scaledDelay + 1) / scaledDelay;
      long newAverage =
          (long) ((1.0 - prevWeight) * newLatencyNanos + prevWeight * previous.average);

      return new TimestampedAverage(currentTimestamp, newAverage, nbMeasure);
    }

    public TimestampedAverage getCurrentAverage() {
      return current.get();
    }
  }

  private void compareAndSwapSlowReplica(Object[] currentNodes, int i, int j) {
    NodeLatencyTracker tracker1 = latencies.getUnchecked((Node) currentNodes[i]);
    NodeLatencyTracker tracker2 = latencies.getUnchecked((Node) currentNodes[j]);
    if (tracker1 != null && tracker2 != null) {
      TimestampedAverage average1 = tracker1.getCurrentAverage();
      TimestampedAverage average2 = tracker2.getCurrentAverage();
      if (average1 != null
          && average2 != null
          && average1.average > average2.average
          && System.nanoTime() - average1.timestamp < RETRY_PERIOD) {
        ArrayUtils.swap(currentNodes, i, j);
      }
    }
  }
}
