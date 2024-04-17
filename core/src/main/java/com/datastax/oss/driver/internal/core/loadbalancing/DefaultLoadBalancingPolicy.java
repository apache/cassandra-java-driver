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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.MandatoryLocalDcHelper;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.BitSet;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLongArray;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default load balancing policy implementation.
 *
 * <p>To activate this policy, modify the {@code basic.load-balancing-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   basic.load-balancing-policy {
 *     class = DefaultLoadBalancingPolicy
 *     local-datacenter = datacenter1
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 *
 * <p><b>Local datacenter</b>: This implementation requires a local datacenter to be defined,
 * otherwise it will throw an {@link IllegalStateException}. A local datacenter can be supplied
 * either:
 *
 * <ol>
 *   <li>Programmatically with {@link
 *       com.datastax.oss.driver.api.core.session.SessionBuilder#withLocalDatacenter(String)
 *       SessionBuilder#withLocalDatacenter(String)};
 *   <li>Through configuration, by defining the option {@link
 *       DefaultDriverOption#LOAD_BALANCING_LOCAL_DATACENTER
 *       basic.load-balancing-policy.local-datacenter};
 *   <li>Or implicitly, if and only if no explicit contact points were provided: in this case this
 *       implementation will infer the local datacenter from the implicit contact point (localhost).
 * </ol>
 *
 * <p><b>Query plan</b>: This implementation prioritizes replica nodes over non-replica ones; if
 * more than one replica is available, the replicas will be shuffled; if more than 2 replicas are
 * available, they will be ordered from most healthy to least healthy ("Power of 2 choices" or busy
 * node avoidance algorithm). Non-replica nodes will be included in a round-robin fashion. If the
 * local datacenter is defined (see above), query plans will only include local nodes, never remote
 * ones; if it is unspecified however, query plans may contain nodes from different datacenters.
 */
@ThreadSafe
public class DefaultLoadBalancingPolicy extends BasicLoadBalancingPolicy implements RequestTracker {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancingPolicy.class);

  private static final long NEWLY_UP_INTERVAL_NANOS = MINUTES.toNanos(1);
  private static final int MAX_IN_FLIGHT_THRESHOLD = 10;
  private static final long RESPONSE_COUNT_RESET_INTERVAL_NANOS = MILLISECONDS.toNanos(200);

  protected final Map<Node, AtomicLongArray> responseTimes = new ConcurrentHashMap<>();
  protected final Map<Node, Long> upTimes = new ConcurrentHashMap<>();
  private final boolean avoidSlowReplicas;

  public DefaultLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
    super(context, profileName);
    this.avoidSlowReplicas =
        profile.getBoolean(DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, true);
  }

  @NonNull
  @Override
  public Optional<RequestTracker> getRequestTracker() {
    if (avoidSlowReplicas) {
      return Optional.of(this);
    } else {
      return Optional.empty();
    }
  }

  @NonNull
  @Override
  protected Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
    return new MandatoryLocalDcHelper(context, profile, logPrefix).discoverLocalDc(nodes);
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    if (!avoidSlowReplicas) {
      return super.newQueryPlan(request, session);
    }

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

          // Test replicas health
          Node newestUpReplica = null;
          BitSet unhealthyReplicas = null; // bit mask storing indices of unhealthy replicas
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
            if (newestUpReplica == null && isUnhealthy(node, session, now)) {
              if (unhealthyReplicas == null) {
                unhealthyReplicas = new BitSet(replicaCount);
              }
              unhealthyReplicas.set(i);
            }
          }

          // When:
          // - there isn't any newly UP replica and
          // - there is one or more unhealthy replicas and
          // - there is a majority of healthy replicas
          int unhealthyReplicasCount =
              unhealthyReplicas == null ? 0 : unhealthyReplicas.cardinality();
          if (newestUpReplica == null
              && unhealthyReplicasCount > 0
              && unhealthyReplicasCount < (replicaCount / 2.0)) {

            // Reorder the unhealthy replicas to the back of the list
            // Start from the back of the replicas, then move backwards;
            // stop once all unhealthy replicas are moved to the back.
            int counter = 0;
            for (int i = replicaCount - 1; i >= 0 && counter < unhealthyReplicasCount; i--) {
              if (unhealthyReplicas.get(i)) {
                ArrayUtils.bubbleDown(currentNodes, i, replicaCount - 1 - counter);
                counter++;
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
    updateResponseTimes(node);
  }

  @Override
  public void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    if (!(error instanceof DriverTimeoutException)) {
      updateResponseTimes(node);
    }
  }

  /** Exposed as a protected method so that it can be accessed by tests */
  protected long nanoTime() {
    return System.nanoTime();
  }

  /** Exposed as a protected method so that it can be accessed by tests */
  protected int diceRoll1d4() {
    return ThreadLocalRandom.current().nextInt(4);
  }

  protected boolean isUnhealthy(@NonNull Node node, @NonNull Session session, long now) {
    return isBusy(node, session) && isResponseRateInsufficient(node, now);
  }

  protected boolean isBusy(@NonNull Node node, @NonNull Session session) {
    return getInFlight(node, session) >= MAX_IN_FLIGHT_THRESHOLD;
  }

  protected boolean isResponseRateInsufficient(@NonNull Node node, long now) {
    // response rate is considered insufficient when less than 2 responses were obtained in
    // the past interval delimited by RESPONSE_COUNT_RESET_INTERVAL_NANOS.
    if (responseTimes.containsKey(node)) {
      AtomicLongArray array = responseTimes.get(node);
      if (array.length() == 2) {
        long threshold = now - RESPONSE_COUNT_RESET_INTERVAL_NANOS;
        long leastRecent = array.get(0);
        return leastRecent - threshold < 0;
      }
    }
    return true;
  }

  protected void updateResponseTimes(@NonNull Node node) {
    responseTimes.compute(
        node,
        (n, array) -> {
          // The array stores at most two timestamps, since we don't need more;
          // the first one is always the least recent one, and hence the one to inspect.
          long now = nanoTime();
          if (array == null) {
            array = new AtomicLongArray(1);
            array.set(0, now);
          } else if (array.length() == 1) {
            long previous = array.get(0);
            array = new AtomicLongArray(2);
            array.set(0, previous);
            array.set(1, now);
          } else {
            array.set(0, array.get(1));
            array.set(1, now);
          }
          return array;
        });
  }

  protected int getInFlight(@NonNull Node node, @NonNull Session session) {
    // The cast will always succeed because there's no way to replace the internal session impl
    ChannelPool pool = ((DefaultSession) session).getPools().get(node);
    // Note: getInFlight() includes orphaned ids, which is what we want as we need to account
    // for requests that were cancelled or timed out (since the node is likely to still be
    // processing them).
    return (pool == null) ? 0 : pool.getInFlight();
  }
}
