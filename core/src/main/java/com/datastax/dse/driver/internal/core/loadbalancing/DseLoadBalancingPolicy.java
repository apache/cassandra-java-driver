/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.loadbalancing;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.dse.driver.internal.core.tracker.MultiplexingRequestTracker;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.Reflection;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DSE load balancing policy implementation.
 *
 * <p>To activate this policy, modify the {@code basic.load-balancing-policy} section in the DSE
 * driver configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   basic.load-balancing-policy {
 *     class = com.datastax.dse.driver.internal.core.loadbalancing.DseLoadBalancingPolicy
 *     local-datacenter = datacenter1
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or OSS driver JAR) and {@code dse-reference.conf} (in
 * the manual or DSE driver JAR) for more details.
 */
@ThreadSafe
public class DseLoadBalancingPolicy implements LoadBalancingPolicy, RequestTracker {

  private static final Logger LOG = LoggerFactory.getLogger(DseLoadBalancingPolicy.class);

  private static final Predicate<Node> INCLUDE_ALL_NODES = n -> true;
  private static final IntUnaryOperator INCREMENT = i -> (i == Integer.MAX_VALUE) ? 0 : i + 1;

  private static final long NEWLY_UP_INTERVAL_NANOS = MINUTES.toNanos(1);
  private static final int MAX_IN_FLIGHT_THRESHOLD = 10;
  private static final long RESPONSE_COUNT_RESET_INTERVAL_NANOS = MILLISECONDS.toNanos(200);

  @NonNull private final String logPrefix;
  @NonNull private final MetadataManager metadataManager;
  @NonNull private final Predicate<Node> filter;
  private final boolean isDefaultPolicy;

  @Nullable @VisibleForTesting volatile String localDc;
  @NonNull private volatile DistanceReporter distanceReporter = (node, distance) -> {};

  private final AtomicInteger roundRobinAmount = new AtomicInteger();
  @VisibleForTesting final CopyOnWriteArraySet<Node> localDcLiveNodes = new CopyOnWriteArraySet<>();
  @VisibleForTesting final Map<Node, AtomicLongArray> responseTimes = new ConcurrentHashMap<>();
  @VisibleForTesting final Map<Node, Long> upTimes = new ConcurrentHashMap<>();

  public DseLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
    this.logPrefix = context.getSessionName() + "|" + profileName;
    this.metadataManager = ((InternalDriverContext) context).getMetadataManager();
    this.isDefaultPolicy = profileName.equals(DriverExecutionProfile.DEFAULT_NAME);
    this.localDc = getLocalDcFromConfig((InternalDriverContext) context, profileName);
    Predicate<Node> filterFromConfig = getFilterFromConfig(context, profileName);
    this.filter =
        node -> {
          String localDc = this.localDc;
          if (localDc != null && !localDc.equals(node.getDatacenter())) {
            LOG.debug(
                "[{}] Ignoring {} because it doesn't belong to the local DC {}",
                logPrefix,
                node,
                localDc);
            return false;
          } else if (!filterFromConfig.test(node)) {
            LOG.debug(
                "[{}] Ignoring {} because it doesn't match the user-provided predicate",
                logPrefix,
                node);
            return false;
          } else {
            return true;
          }
        };
    ((MultiplexingRequestTracker) context.getRequestTracker()).register(this);
  }

  @Override
  public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
    this.distanceReporter = distanceReporter;

    Set<DefaultNode> contactPoints = metadataManager.getContactPoints();
    if (localDc == null) {
      if (metadataManager.wasImplicitContactPoint()) {
        // No explicit contact points provided => the driver used the default (127.0.0.1:9042), and
        // we allow inferring the local DC in this case
        assert contactPoints.size() == 1;
        Node contactPoint = contactPoints.iterator().next();
        localDc = contactPoint.getDatacenter();
        LOG.debug("[{}] Local DC set from contact point {}: {}", logPrefix, contactPoint, localDc);
      } else {
        throw new IllegalStateException(
            "You provided explicit contact points, the local DC must be specified (see "
                + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
                + " in the config)");
      }
    } else {
      ImmutableMap.Builder<Node, String> builder = ImmutableMap.builder();
      for (Node node : contactPoints) {
        String datacenter = node.getDatacenter();
        if (!Objects.equals(localDc, datacenter)) {
          builder.put(node, (datacenter == null) ? "<null>" : datacenter);
        }
      }
      ImmutableMap<Node, String> badContactPoints = builder.build();
      if (isDefaultPolicy && !badContactPoints.isEmpty()) {
        LOG.warn(
            "[{}] You specified {} as the local DC, but some contact points are from a different DC ({})",
            logPrefix,
            localDc,
            badContactPoints);
      }
    }

    for (Node node : nodes.values()) {
      if (filter.test(node)) {
        distanceReporter.setDistance(node, NodeDistance.LOCAL);
        if (node.getState() != NodeState.DOWN) {
          // This includes state == UNKNOWN. If the node turns out to be unreachable, this will be
          // detected when we try to open a pool to it, it will get marked down and this will be
          // signaled back to this policy
          localDcLiveNodes.add(node);
        }
      } else {
        distanceReporter.setDistance(node, NodeDistance.IGNORED);
      }
    }
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    // Take a snapshot since the set is concurrent:
    Object[] currentNodes = localDcLiveNodes.toArray();

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

    return new QueryPlan(currentNodes);
  }

  @Override
  public void onAdd(@NonNull Node node) {
    if (filter.test(node)) {
      LOG.debug("[{}] {} was added, setting distance to LOCAL", logPrefix, node);
      // Setting to a non-ignored distance triggers the session to open a pool, which will in turn
      // set the node UP when the first channel gets opened.
      distanceReporter.setDistance(node, NodeDistance.LOCAL);
    } else {
      distanceReporter.setDistance(node, NodeDistance.IGNORED);
    }
  }

  @Override
  public void onUp(@NonNull Node node) {
    if (filter.test(node)) {
      // Normally this is already the case, but the filter could be dynamic and have ignored the
      // node previously.
      distanceReporter.setDistance(node, NodeDistance.LOCAL);
      if (localDcLiveNodes.add(node)) {
        upTimes.put(node, nanoTime());
        LOG.debug("[{}] {} came back UP, added to live set", logPrefix, node);
      }
    } else {
      distanceReporter.setDistance(node, NodeDistance.IGNORED);
    }
  }

  @Override
  public void onDown(@NonNull Node node) {
    if (localDcLiveNodes.remove(node)) {
      upTimes.remove(node);
      LOG.debug("[{}] {} went DOWN, removed from live set", logPrefix, node);
    }
  }

  @Override
  public void onRemove(@NonNull Node node) {
    if (localDcLiveNodes.remove(node)) {
      upTimes.remove(node);
      LOG.debug("[{}] {} was removed, removed from live set", logPrefix, node);
    }
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
    updateResponseTimes(node);
  }

  @Override
  public void close() {}

  @VisibleForTesting
  void shuffleHead(Object[] array, int n) {
    ArrayUtils.shuffleHead(array, n);
  }

  @VisibleForTesting
  long nanoTime() {
    return System.nanoTime();
  }

  @VisibleForTesting
  int diceRoll1d4() {
    return ThreadLocalRandom.current().nextInt(4);
  }

  private Set<Node> getReplicas(@Nullable Request request, @Nullable Session session) {
    if (request == null || session == null) {
      return Collections.emptySet();
    }

    // Note: we're on the hot path and the getXxx methods are potentially more than simple getters,
    // so we only call each method when strictly necessary (which is why the code below looks a bit
    // weird).
    CqlIdentifier keyspace = request.getKeyspace();
    if (keyspace == null) {
      keyspace = request.getRoutingKeyspace();
    }
    if (keyspace == null && session.getKeyspace().isPresent()) {
      keyspace = session.getKeyspace().get();
    }
    if (keyspace == null) {
      return Collections.emptySet();
    }

    Token token = request.getRoutingToken();
    ByteBuffer key = (token == null) ? request.getRoutingKey() : null;
    if (token == null && key == null) {
      return Collections.emptySet();
    }

    Optional<TokenMap> maybeTokenMap = metadataManager.getMetadata().getTokenMap();
    if (maybeTokenMap.isPresent()) {
      TokenMap tokenMap = maybeTokenMap.get();
      return (token != null)
          ? tokenMap.getReplicas(keyspace, token)
          : tokenMap.getReplicas(keyspace, key);
    } else {
      return Collections.emptySet();
    }
  }

  private boolean isUnhealthy(@NonNull Node node, @NonNull Session session, long now) {
    return isBusy(node, session) && isResponseRateInsufficient(node, now);
  }

  private boolean isBusy(@NonNull Node node, @NonNull Session session) {
    return getInFlight(node, session) >= MAX_IN_FLIGHT_THRESHOLD;
  }

  @VisibleForTesting
  boolean isResponseRateInsufficient(@NonNull Node node, long now) {
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

  private void updateResponseTimes(@NonNull Node node) {
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

  private String getLocalDcFromConfig(
      @NonNull InternalDriverContext context, @NonNull String profileName) {
    // see if the local datacenter has been set programmatically
    String localDataCenter = context.getLocalDatacenter(profileName);
    if (localDataCenter != null) {
      LOG.debug("[{}] Local DC set from builder: {}", logPrefix, localDataCenter);
      return localDataCenter;
    } else {
      // it's not been set programmatically, try to get it from config
      DriverExecutionProfile config = context.getConfig().getProfile(profileName);
      localDataCenter = config.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null);
      if (localDataCenter != null) {
        LOG.debug("[{}] Local DC set from configuration: {}", logPrefix, localDataCenter);
      }
      return localDataCenter;
    }
  }

  private static int getInFlight(@NonNull Node node, @NonNull Session session) {
    // The cast will always succeed because there's no way to replace the internal session impl
    ChannelPool pool = ((DefaultSession) session).getPools().get(node);
    // Note: getInFlight() includes orphaned ids, which is what we want as we need to account
    // for requests that were cancelled or timed out (since the node is likely to still be
    // processing them).
    return (pool == null) ? 0 : pool.getInFlight();
  }

  private static Predicate<Node> getFilterFromConfig(
      @NonNull DriverContext context, @NonNull String profileName) {
    Predicate<Node> filterFromBuilder =
        ((InternalDriverContext) context).getNodeFilter(profileName);
    if (filterFromBuilder != null) {
      return filterFromBuilder;
    } else {
      @SuppressWarnings("unchecked")
      Predicate<Node> filter =
          Reflection.buildFromConfig(
                  (InternalDriverContext) context,
                  profileName,
                  DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS,
                  Predicate.class)
              .orElse(INCLUDE_ALL_NODES);
      return filter;
    }
  }
}
