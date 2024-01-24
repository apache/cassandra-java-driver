/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.loadbalancing;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.DefaultNodeDistanceEvaluatorHelper;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.OptionalLocalDcHelper;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.DcAgnosticNodeSet;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.MultiDcNodeSet;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.NodeSet;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.SingleDcNodeSet;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.CompositeQueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.LazyQueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.driver.shaded.guava.common.base.Predicates;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A basic implementation of {@link LoadBalancingPolicy} that can serve as a building block for more
 * advanced use cases.
 *
 * <p>To activate this policy, modify the {@code basic.load-balancing-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   basic.load-balancing-policy {
 *     class = BasicLoadBalancingPolicy
 *     local-datacenter = datacenter1 # optional
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 *
 * <p><b>Local datacenter</b>: This implementation will only define a local datacenter if it is
 * explicitly set either through configuration or programmatically; if the local datacenter is
 * unspecified, this implementation will effectively act as a datacenter-agnostic load balancing
 * policy and will consider all nodes in the cluster when creating query plans, regardless of their
 * datacenter.
 *
 * <p><b>Query plan</b>: This implementation prioritizes replica nodes over non-replica ones; if
 * more than one replica is available, the replicas will be shuffled. Non-replica nodes will be
 * included in a round-robin fashion. If the local datacenter is defined (see above), query plans
 * will only include local nodes, never remote ones; if it is unspecified however, query plans may
 * contain nodes from different datacenters.
 *
 * <p><b>This class is not recommended for normal users who should always prefer {@link
 * DefaultLoadBalancingPolicy}</b>.
 */
@ThreadSafe
public class BasicLoadBalancingPolicy implements LoadBalancingPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(BasicLoadBalancingPolicy.class);

  protected static final IntUnaryOperator INCREMENT = i -> (i == Integer.MAX_VALUE) ? 0 : i + 1;
  private static final Object[] EMPTY_NODES = new Object[0];

  @NonNull protected final InternalDriverContext context;
  @NonNull protected final DriverExecutionProfile profile;
  @NonNull protected final String logPrefix;

  protected final AtomicInteger roundRobinAmount = new AtomicInteger();

  protected final int maxNodesPerRemoteDc;
  private final boolean allowDcFailoverForLocalCl;
  private final ConsistencyLevel defaultConsistencyLevel;

  // private because they should be set in init() and never be modified after
  // Yugabyte specific: protected because distanceReporter is to be used in
  // YugabyteDefaultLoadBalancingPolicy class
  protected volatile DistanceReporter distanceReporter;
  protected volatile NodeDistanceEvaluator nodeDistanceEvaluator;
  protected volatile String localDc;
  protected volatile NodeSet liveNodes;

  public BasicLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
    this.context = (InternalDriverContext) context;
    profile = context.getConfig().getProfile(profileName);
    logPrefix = context.getSessionName() + "|" + profileName;
    maxNodesPerRemoteDc =
        profile.getInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC);
    allowDcFailoverForLocalCl =
        profile.getBoolean(
            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS);
    defaultConsistencyLevel =
        this.context
            .getConsistencyLevelRegistry()
            .nameToLevel(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY));
  }

  /**
   * Returns the local datacenter name, if known; empty otherwise.
   *
   * <p>When this method returns null, then datacenter awareness is completely disabled. All
   * non-ignored nodes will be considered "local" regardless of their actual datacenters, and will
   * have equal chances of being selected for query plans.
   *
   * <p>After the policy is {@linkplain #init(Map, DistanceReporter) initialized} this method will
   * return the local datacenter that was discovered by calling {@link #discoverLocalDc(Map)}.
   * Before initialization, this method always returns null.
   */
  @Nullable
  protected String getLocalDatacenter() {
    return localDc;
  }

  /** @return The nodes currently considered as live. */
  protected NodeSet getLiveNodes() {
    return liveNodes;
  }

  @Override
  public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
    this.distanceReporter = distanceReporter;
    localDc = discoverLocalDc(nodes).orElse(null);
    nodeDistanceEvaluator = createNodeDistanceEvaluator(localDc, nodes);
    liveNodes =
        localDc == null
            ? new DcAgnosticNodeSet()
            : maxNodesPerRemoteDc <= 0 ? new SingleDcNodeSet(localDc) : new MultiDcNodeSet();
    for (Node node : nodes.values()) {
      NodeDistance distance = computeNodeDistance(node);
      distanceReporter.setDistance(node, distance);
      if (distance != NodeDistance.IGNORED && node.getState() != NodeState.DOWN) {
        // This includes state == UNKNOWN. If the node turns out to be unreachable, this will be
        // detected when we try to open a pool to it, it will get marked down and this will be
        // signaled back to this policy, which will then remove it from the live set.
        liveNodes.add(node);
      }
    }
  }

  /**
   * Returns the local datacenter, if it can be discovered, or returns {@link Optional#empty empty}
   * otherwise.
   *
   * <p>This method is called only once, during {@linkplain LoadBalancingPolicy#init(Map,
   * LoadBalancingPolicy.DistanceReporter) initialization}.
   *
   * <p>Implementors may choose to throw {@link IllegalStateException} instead of returning {@link
   * Optional#empty empty}, if they require a local datacenter to be defined in order to operate
   * properly.
   *
   * <p>If this method returns empty, then datacenter awareness will be completely disabled. All
   * non-ignored nodes will be considered "local" regardless of their actual datacenters, and will
   * have equal chances of being selected for query plans.
   *
   * @param nodes All the nodes that were known to exist in the cluster (regardless of their state)
   *     when the load balancing policy was initialized. This argument is provided in case
   *     implementors need to inspect the cluster topology to discover the local datacenter.
   * @return The local datacenter, or {@link Optional#empty empty} if none found.
   * @throws IllegalStateException if the local datacenter could not be discovered, and this policy
   *     cannot operate without it.
   */
  @NonNull
  protected Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
    return new OptionalLocalDcHelper(context, profile, logPrefix).discoverLocalDc(nodes);
  }

  /**
   * Creates a new node distance evaluator to use with this policy.
   *
   * <p>This method is called only once, during {@linkplain LoadBalancingPolicy#init(Map,
   * LoadBalancingPolicy.DistanceReporter) initialization}, and only after local datacenter
   * discovery has been attempted.
   *
   * @param localDc The local datacenter that was just discovered, or null if none found.
   * @param nodes All the nodes that were known to exist in the cluster (regardless of their state)
   *     when the load balancing policy was initialized. This argument is provided in case
   *     implementors need to inspect the cluster topology to create the evaluator.
   * @return the distance evaluator to use.
   */
  @NonNull
  protected NodeDistanceEvaluator createNodeDistanceEvaluator(
      @Nullable String localDc, @NonNull Map<UUID, Node> nodes) {
    return new DefaultNodeDistanceEvaluatorHelper(context, profile, logPrefix)
        .createNodeDistanceEvaluator(localDc, nodes);
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    // Take a snapshot since the set is concurrent:
    Object[] currentNodes = liveNodes.dc(localDc).toArray();

    Set<Node> allReplicas = getReplicas(request, session);
    int replicaCount = 0; // in currentNodes

    if (!allReplicas.isEmpty()) {
      // Move replicas to the beginning
      for (int i = 0; i < currentNodes.length; i++) {
        Node node = (Node) currentNodes[i];
        if (allReplicas.contains(node)) {
          ArrayUtils.bubbleUp(currentNodes, i, replicaCount);
          replicaCount += 1;
        }
      }

      if (replicaCount > 1) {
        shuffleHead(currentNodes, replicaCount);
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

  @NonNull
  protected Set<Node> getReplicas(@Nullable Request request, @Nullable Session session) {
    if (request == null || session == null) {
      return Collections.emptySet();
    }

    Optional<TokenMap> maybeTokenMap = context.getMetadataManager().getMetadata().getTokenMap();
    if (!maybeTokenMap.isPresent()) {
      return Collections.emptySet();
    }

    // Note: we're on the hot path and the getXxx methods are potentially more than simple getters,
    // so we only call each method when strictly necessary (which is why the code below looks a bit
    // weird).
    CqlIdentifier keyspace;
    Token token;
    ByteBuffer key;
    try {
      keyspace = request.getKeyspace();
      if (keyspace == null) {
        keyspace = request.getRoutingKeyspace();
      }
      if (keyspace == null && session.getKeyspace().isPresent()) {
        keyspace = session.getKeyspace().get();
      }
      if (keyspace == null) {
        return Collections.emptySet();
      }

      token = request.getRoutingToken();
      key = (token == null) ? request.getRoutingKey() : null;
      if (token == null && key == null) {
        return Collections.emptySet();
      }
    } catch (Exception e) {
      // Protect against poorly-implemented Request instances
      LOG.error("Unexpected error while trying to compute query plan", e);
      return Collections.emptySet();
    }

    TokenMap tokenMap = maybeTokenMap.get();
    return token != null
        ? tokenMap.getReplicas(keyspace, token)
        : tokenMap.getReplicas(keyspace, key);
  }

  @NonNull
  protected Queue<Node> maybeAddDcFailover(@Nullable Request request, @NonNull Queue<Node> local) {
    if (maxNodesPerRemoteDc <= 0 || localDc == null) {
      return local;
    }
    if (!allowDcFailoverForLocalCl && request instanceof Statement) {
      Statement<?> statement = (Statement<?>) request;
      ConsistencyLevel consistency = statement.getConsistencyLevel();
      if (consistency == null) {
        consistency = defaultConsistencyLevel;
      }
      if (consistency.isDcLocal()) {
        return local;
      }
    }
    QueryPlan remote =
        new LazyQueryPlan() {

          @Override
          protected Object[] computeNodes() {
            Object[] remoteNodes =
                liveNodes.dcs().stream()
                    .filter(Predicates.not(Predicates.equalTo(localDc)))
                    .flatMap(dc -> liveNodes.dc(dc).stream().limit(maxNodesPerRemoteDc))
                    .toArray();

            int remoteNodesLength = remoteNodes.length;
            if (remoteNodesLength == 0) {
              return EMPTY_NODES;
            }
            shuffleHead(remoteNodes, remoteNodesLength);
            return remoteNodes;
          }
        };

    return new CompositeQueryPlan(local, remote);
  }

  /** Exposed as a protected method so that it can be accessed by tests */
  protected void shuffleHead(Object[] currentNodes, int headLength) {
    ArrayUtils.shuffleHead(currentNodes, headLength);
  }

  @Override
  public void onAdd(@NonNull Node node) {
    NodeDistance distance = computeNodeDistance(node);
    // Setting to a non-ignored distance triggers the session to open a pool, which will in turn
    // set the node UP when the first channel gets opened, then #onUp will be called, and the
    // node will be eventually added to the live set.
    distanceReporter.setDistance(node, distance);
    LOG.debug("[{}] {} was added, setting distance to {}", logPrefix, node, distance);
  }

  @Override
  public void onUp(@NonNull Node node) {
    NodeDistance distance = computeNodeDistance(node);
    if (node.getDistance() != distance) {
      distanceReporter.setDistance(node, distance);
    }
    if (distance != NodeDistance.IGNORED && liveNodes.add(node)) {
      LOG.debug("[{}] {} came back UP, added to live set", logPrefix, node);
    }
  }

  @Override
  public void onDown(@NonNull Node node) {
    if (liveNodes.remove(node)) {
      LOG.debug("[{}] {} went DOWN, removed from live set", logPrefix, node);
    }
  }

  @Override
  public void onRemove(@NonNull Node node) {
    if (liveNodes.remove(node)) {
      LOG.debug("[{}] {} was removed, removed from live set", logPrefix, node);
    }
  }

  /**
   * Computes the distance of the given node.
   *
   * <p>This method is called during {@linkplain #init(Map, DistanceReporter) initialization}, when
   * a node {@linkplain #onAdd(Node) is added}, and when a node {@linkplain #onUp(Node) is back UP}.
   */
  protected NodeDistance computeNodeDistance(@NonNull Node node) {
    // We interrogate the custom evaluator every time since it could be dynamic
    // and change its verdict between two invocations of this method.
    NodeDistance distance = nodeDistanceEvaluator.evaluateDistance(node, localDc);
    if (distance != null) {
      return distance;
    }
    // no local DC defined: all nodes are considered LOCAL.
    if (localDc == null) {
      return NodeDistance.LOCAL;
    }
    // otherwise, the node is LOCAL if its datacenter is the local datacenter.
    if (Objects.equals(node.getDatacenter(), localDc)) {
      return NodeDistance.LOCAL;
    }
    // otherwise, the node will be either REMOTE or IGNORED, depending
    // on how many remote nodes we accept per DC.
    if (maxNodesPerRemoteDc > 0) {
      Object[] remoteNodes = liveNodes.dc(node.getDatacenter()).toArray();
      for (int i = 0; i < maxNodesPerRemoteDc; i++) {
        if (i == remoteNodes.length) {
          // there is still room for one more REMOTE node in this DC
          return NodeDistance.REMOTE;
        } else if (remoteNodes[i] == node) {
          return NodeDistance.REMOTE;
        }
      }
    }
    return NodeDistance.IGNORED;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
