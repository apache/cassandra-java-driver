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

import com.datastax.oss.driver.api.core.CqlIdentifier;
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
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.DefaultNodeFilterHelper;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.OptionalLocalDcHelper;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
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

  @NonNull protected final InternalDriverContext context;
  @NonNull protected final DriverExecutionProfile profile;
  @NonNull protected final String logPrefix;

  protected final AtomicInteger roundRobinAmount = new AtomicInteger();
  protected final CopyOnWriteArraySet<Node> liveNodes = new CopyOnWriteArraySet<>();

  // private because they should be set in init() and never be modified after
  private volatile DistanceReporter distanceReporter;
  private volatile Predicate<Node> filter;
  private volatile String localDc;

  public BasicLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
    this.context = (InternalDriverContext) context;
    profile = context.getConfig().getProfile(profileName);
    logPrefix = context.getSessionName() + "|" + profileName;
  }

  /** @return The local datacenter, if known; empty otherwise. */
  public Optional<String> getLocalDatacenter() {
    return Optional.ofNullable(localDc);
  }

  /**
   * @return An immutable copy of the nodes currently considered as live; if the local datacenter is
   *     known, this set will contain only nodes belonging to that datacenter.
   */
  public Set<Node> getLiveNodes() {
    return ImmutableSet.copyOf(liveNodes);
  }

  @Override
  public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
    this.distanceReporter = distanceReporter;
    localDc = discoverLocalDc(nodes).orElse(null);
    filter = createNodeFilter(localDc, nodes);
    for (Node node : nodes.values()) {
      if (filter.test(node)) {
        distanceReporter.setDistance(node, NodeDistance.LOCAL);
        if (node.getState() != NodeState.DOWN) {
          // This includes state == UNKNOWN. If the node turns out to be unreachable, this will be
          // detected when we try to open a pool to it, it will get marked down and this will be
          // signaled back to this policy
          liveNodes.add(node);
        }
      } else {
        distanceReporter.setDistance(node, NodeDistance.IGNORED);
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
   * Creates a new node filter to use with this policy.
   *
   * <p>This method is called only once, during {@linkplain LoadBalancingPolicy#init(Map,
   * LoadBalancingPolicy.DistanceReporter) initialization}, and only after local datacenter
   * discovery has been attempted.
   *
   * @param localDc The local datacenter that was just discovered, or null if none found.
   * @param nodes All the nodes that were known to exist in the cluster (regardless of their state)
   *     when the load balancing policy was initialized. This argument is provided in case
   *     implementors need to inspect the cluster topology to create the node filter.
   * @return the node filter to use.
   */
  @NonNull
  protected Predicate<Node> createNodeFilter(
      @Nullable String localDc, @NonNull Map<UUID, Node> nodes) {
    return new DefaultNodeFilterHelper(context, profile, logPrefix)
        .createNodeFilter(localDc, nodes);
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    // Take a snapshot since the set is concurrent:
    Object[] currentNodes = liveNodes.toArray();

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

    return new QueryPlan(currentNodes);
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
    CqlIdentifier keyspace = null;
    Token token = null;
    ByteBuffer key = null;
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

  /** Exposed as a protected method so that it can be accessed by tests */
  protected void shuffleHead(Object[] currentNodes, int replicaCount) {
    ArrayUtils.shuffleHead(currentNodes, replicaCount);
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
      if (liveNodes.add(node)) {
        LOG.debug("[{}] {} came back UP, added to live set", logPrefix, node);
      }
    } else {
      distanceReporter.setDistance(node, NodeDistance.IGNORED);
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

  @Override
  public void close() {
    // nothing to do
  }
}
