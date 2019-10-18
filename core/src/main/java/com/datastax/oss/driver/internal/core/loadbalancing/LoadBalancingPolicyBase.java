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
import java.util.HashSet;
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
 * A basic implementation of {@link LoadBalancingPolicy} that can serve as a building block for
 * advanced use cases.
 *
 * <p>This class is not recommended for normal users who should always prefer {@link
 * DefaultLoadBalancingPolicy}.
 */
@ThreadSafe
public abstract class LoadBalancingPolicyBase implements LoadBalancingPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(LoadBalancingPolicyBase.class);

  protected static final Predicate<Node> INCLUDE_ALL_NODES = n -> true;
  protected static final IntUnaryOperator INCREMENT = i -> (i == Integer.MAX_VALUE) ? 0 : i + 1;

  @NonNull protected final String logPrefix;
  @NonNull protected final InternalDriverContext context;
  @NonNull protected final String profileName;
  @NonNull protected final DriverExecutionProfile profile;

  protected final AtomicInteger roundRobinAmount = new AtomicInteger();
  protected final CopyOnWriteArraySet<Node> localDcLiveNodes = new CopyOnWriteArraySet<>();

  protected volatile DistanceReporter distanceReporter;
  protected volatile Predicate<Node> filter;
  protected volatile String localDc;

  protected LoadBalancingPolicyBase(
      @NonNull InternalDriverContext context, @NonNull String profileName) {
    this.context = context;
    this.profileName = profileName;
    profile = context.getConfig().getProfile(profileName);
    logPrefix = context.getSessionName() + "|" + profileName;
  }

  public String getLocalDatacenter() {
    return localDc;
  }

  public Set<Node> getLocalDcLiveNodes() {
    return ImmutableSet.copyOf(localDcLiveNodes);
  }

  @Override
  public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
    this.distanceReporter = distanceReporter;
    filter = createNodeFilter();
    localDc = computeLocalDatacenter();
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

  /**
   * Returns the node filter to use with this policy.
   *
   * <p>This method should be called upon {@linkplain #init(Map, DistanceReporter) initialization}.
   *
   * <p>The default implementation fetches the user-supplied filter, if any, from the programmatic
   * configuration API, or else, from the driver configuration. If no user-supplied filter can be
   * retrieved, a dummy filter will be used which accepts all nodes unconditionally.
   *
   * <p>Note that, regardless of the filter supplied by the end user, the filter returned by this
   * implementation will always reject nodes that report a datacenter different from the local
   * datacenter.
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
    return wrapNodeFilter(filter);
  }

  /**
   * Wraps the user-supplied filter within a filter that always rejects nodes belonging to non-local
   * datacenters.
   *
   * @param userFilter The user-supplied filter.
   * @return The wrapped filter.
   */
  @NonNull
  protected Predicate<Node> wrapNodeFilter(@NonNull Predicate<Node> userFilter) {
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
   * Returns the local datacenter to use with this policy.
   *
   * <p>This method should be called upon {@linkplain #init(Map, DistanceReporter) initialization}.
   *
   * <p>The default implementation fetches the user-supplied datacenter, if any, from the
   * programmatic configuration API, or else, from the driver configuration. If no user-supplied
   * datacenter can be retrieved, an attempt to infer the datacenter from the contact points will be
   * made. If no datacenter can be inferred from the contact points, an {@link
   * IllegalStateException} is thrown.
   *
   * @return The local datacenter.
   * @throws IllegalStateException if the local datacenter cannot be inferred.
   */
  @NonNull
  protected String computeLocalDatacenter() {
    String localDc = context.getLocalDatacenter(profileName);
    if (localDc != null) {
      LOG.debug("[{}] Local DC set programmatically: {}", logPrefix, localDc);
      checkLocalDatacenter(localDc);
    } else if (profile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)) {
      localDc = profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER);
      LOG.debug("[{}] Local DC set from configuration: {}", logPrefix, localDc);
      checkLocalDatacenter(localDc);
    } else {
      localDc = inferLocalDatacenter();
    }
    return localDc;
  }

  /**
   * Checks if the contact points are compatible with the local datacenter specified either through
   * configuration, or programmatically.
   *
   * <p>The default implementation logs a warning when a contact point reports a datacenter
   * different from the local one.
   *
   * @param localDc The local datacenter, as specified in the config, or programmatically.
   */
  protected void checkLocalDatacenter(@NonNull String localDc) {
    Set<? extends Node> contactPoints = context.getMetadataManager().getContactPoints();
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
   * Infers the local datacenter when no local datacenter was specified neither through
   * configuration nor programmatically.
   *
   * <p>The default implementation infers the local datacenter from the contact points: if all
   * contact points share the same datacenter, that datacenter is returned. If the contact points
   * are from different datacenters, or if no contact points reported any datacenter, an {@link
   * IllegalStateException} is thrown.
   *
   * @return The inferred local datacenter.
   * @throws IllegalStateException if the local datacenter cannot be inferred.
   */
  @NonNull
  protected String inferLocalDatacenter() {
    Set<String> datacenters = new HashSet<>();
    Set<? extends Node> contactPoints = context.getMetadataManager().getContactPoints();
    for (Node node : contactPoints) {
      String datacenter = node.getDatacenter();
      if (datacenter != null) {
        datacenters.add(datacenter);
      }
    }
    if (datacenters.size() == 1) {
      String localDc = datacenters.iterator().next();
      LOG.info("[{}] Inferred local DC from contact points: {}", logPrefix, localDc);
      return localDc;
    }
    if (datacenters.isEmpty()) {
      throw new IllegalStateException(
          "The local DC could not be inferred from contact points, please set it explicitly (see "
              + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
              + " in the config, or set it programmatically with SessionBuilder.withLocalDatacenter)");
    }
    throw new IllegalStateException(
        String.format(
            "No local DC was provided, but the contact points are from different DCs: %s; "
                + "please set the local DC explicitly (see "
                + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
                + " in the config, or set it programmatically with SessionBuilder.withLocalDatacenter)",
            formatNodes(contactPoints)));
  }

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
    Object[] currentNodes = localDcLiveNodes.toArray();

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
      if (localDcLiveNodes.add(node)) {
        LOG.debug("[{}] {} came back UP, added to live set", logPrefix, node);
      }
    } else {
      distanceReporter.setDistance(node, NodeDistance.IGNORED);
    }
  }

  @Override
  public void onDown(@NonNull Node node) {
    if (localDcLiveNodes.remove(node)) {
      LOG.debug("[{}] {} went DOWN, removed from live set", logPrefix, node);
    }
  }

  @Override
  public void onRemove(@NonNull Node node) {
    if (localDcLiveNodes.remove(node)) {
      LOG.debug("[{}] {} was removed, removed from live set", logPrefix, node);
    }
  }

  @Override
  public void close() {
    // nothing to do
  }
}
