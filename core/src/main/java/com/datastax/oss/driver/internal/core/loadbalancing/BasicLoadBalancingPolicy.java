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
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.Reflection;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

  protected static final Predicate<Node> INCLUDE_ALL_NODES = n -> true;
  protected static final IntUnaryOperator INCREMENT = i -> (i == Integer.MAX_VALUE) ? 0 : i + 1;

  @NonNull protected final String logPrefix;
  @NonNull protected final InternalDriverContext context;
  @NonNull protected final String profileName;
  @NonNull protected final DriverExecutionProfile profile;

  protected final AtomicInteger roundRobinAmount = new AtomicInteger();
  protected final CopyOnWriteArraySet<Node> liveNodes = new CopyOnWriteArraySet<>();

  protected volatile DistanceReporter distanceReporter;
  protected volatile Predicate<Node> filter;
  protected volatile String localDc;

  public BasicLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
    this.context = (InternalDriverContext) context;
    this.profileName = profileName;
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
    filter = createNodeFilter();
    localDc = discoverLocalDatacenter().orElse(null);
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
   * Returns the node filter to use with this policy.
   *
   * <p>This method should be called upon {@linkplain #init(Map, DistanceReporter) initialization}.
   *
   * <p>The default implementation fetches the user-supplied filter, if any, from the programmatic
   * configuration API, or else, from the driver configuration. If no user-supplied filter can be
   * retrieved, a dummy filter will be used which accepts all nodes unconditionally.
   *
   * <p>Note that, regardless of the filter supplied by the end user, if a local datacenter is
   * defined the filter returned by this implementation will always reject nodes that report a
   * datacenter different from the local one.
   *
   * @return the node filter to use.
   */
  @NonNull
  protected Predicate<Node> createNodeFilter() {
    Predicate<Node> filter = context.getNodeFilter(profileName);
    if (filter != null) {
      LOG.debug("[{}] Node filter set programmatically", logPrefix);
    } else {
      @SuppressWarnings("unchecked")
      Predicate<Node> filterFromConfig =
          Reflection.buildFromConfig(
                  context,
                  profileName,
                  DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS,
                  Predicate.class)
              .orElse(INCLUDE_ALL_NODES);
      filter = filterFromConfig;
      LOG.debug("[{}] Node filter set from configuration", logPrefix);
    }
    Predicate<Node> userFilter = filter;
    return node -> {
      String localDc1 = localDc;
      if (localDc1 != null && !localDc1.equals(node.getDatacenter())) {
        LOG.debug(
            "[{}] Ignoring {} because it doesn't belong to the local DC {}",
            logPrefix,
            node,
            localDc1);
        return false;
      } else if (!userFilter.test(node)) {
        LOG.debug(
            "[{}] Ignoring {} because it doesn't match the user-provided predicate",
            logPrefix,
            node);
        return false;
      } else {
        return true;
      }
    };
  }

  /**
   * Discovers the local datacenter to use with this policy.
   *
   * <p>This method should be called upon {@linkplain #init(Map, DistanceReporter) initialization}.
   *
   * <p>This implementation fetches the user-supplied datacenter, if any, from the programmatic
   * configuration API, or else, from the driver configuration. If no user-supplied datacenter can
   * be retrieved, this implementation returns {@link Optional#empty empty}, effectively causing
   * this load balancing policy to operate in a datacenter-agnostic fashion.
   *
   * <p>Subclasses may choose to throw {@link IllegalStateException} instead of returning {@link
   * Optional#empty empty}, if they require a local datacenter to be defined in order to operate
   * properly.
   *
   * @return The local datacenter, or {@link Optional#empty empty} if none found.
   * @throws IllegalStateException if the local datacenter could not be discovered, and this policy
   *     cannot operate without it.
   */
  @NonNull
  protected Optional<String> discoverLocalDatacenter() {
    String localDc = context.getLocalDatacenter(profileName);
    if (localDc != null) {
      LOG.debug("[{}] Local DC set programmatically: {}", logPrefix, localDc);
      checkLocalDatacenterCompatibility(localDc, context.getMetadataManager().getContactPoints());
      return Optional.of(localDc);
    } else if (profile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)) {
      localDc = profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER);
      LOG.debug("[{}] Local DC set from configuration: {}", logPrefix, localDc);
      checkLocalDatacenterCompatibility(localDc, context.getMetadataManager().getContactPoints());
      return Optional.of(localDc);
    } else {
      return Optional.empty();
    }
  }

  /**
   * Checks if the contact points are compatible with the local datacenter specified either through
   * configuration, or programmatically.
   *
   * <p>The default implementation logs a warning when a contact point reports a datacenter
   * different from the local one.
   *
   * @param localDc The local datacenter, as specified in the config, or programmatically.
   * @param contactPoints The contact points provided when creating the session.
   */
  protected void checkLocalDatacenterCompatibility(
      @NonNull String localDc, Set<? extends Node> contactPoints) {
    Set<Node> badContactPoints = new LinkedHashSet<>();
    for (Node node : contactPoints) {
      if (!Objects.equals(localDc, node.getDatacenter())) {
        badContactPoints.add(node);
      }
    }
    if (!badContactPoints.isEmpty()) {
      LOG.warn(
          "[{}] You specified {} as the local DC, but some contact points are from a different DC: {}; "
              + "please provide the correct local DC, or check your contact points",
          logPrefix,
          localDc,
          formatNodes(badContactPoints));
    }
  }

  /**
   * Formats the given nodes as a string detailing each contact point and its datacenter, for
   * informational purposes.
   */
  @NonNull
  protected String formatNodes(Iterable<? extends Node> nodes) {
    List<String> l = new ArrayList<>();
    for (Node node : nodes) {
      l.add(node + "=" + node.getDatacenter());
    }
    return String.join(", ", l);
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

    Optional<TokenMap> maybeTokenMap = context.getMetadataManager().getMetadata().getTokenMap();
    if (maybeTokenMap.isPresent()) {
      TokenMap tokenMap = maybeTokenMap.get();
      return (token != null)
          ? tokenMap.getReplicas(keyspace, token)
          : tokenMap.getReplicas(keyspace, key);
    } else {
      return Collections.emptySet();
    }
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
