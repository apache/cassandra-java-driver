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
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.Reflection;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
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
 */
@ThreadSafe
public class DefaultLoadBalancingPolicy implements LoadBalancingPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancingPolicy.class);
  private static final Predicate<Node> INCLUDE_ALL_NODES = n -> true;
  private static final IntUnaryOperator INCREMENT = i -> (i == Integer.MAX_VALUE) ? 0 : i + 1;

  private final String logPrefix;
  private final MetadataManager metadataManager;
  private final Predicate<Node> filter;
  private final AtomicInteger roundRobinAmount = new AtomicInteger();
  private final boolean isDefaultPolicy;
  @VisibleForTesting final CopyOnWriteArraySet<Node> localDcLiveNodes = new CopyOnWriteArraySet<>();

  private volatile DistanceReporter distanceReporter;
  @VisibleForTesting volatile String localDc;

  public DefaultLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
    this(
        context.getSessionName() + "|" + profileName,
        getLocalDcFromConfig(context, profileName),
        getFilterFromConfig(context, profileName),
        context,
        profileName.equals(DriverExecutionProfile.DEFAULT_NAME));
  }

  @VisibleForTesting
  DefaultLoadBalancingPolicy(
      @NonNull String logPrefix,
      @Nullable String localDcFromConfig,
      @NonNull Predicate<Node> filterFromConfig,
      @NonNull DriverContext context,
      boolean isDefaultPolicy) {
    this.logPrefix = logPrefix;
    this.metadataManager = ((InternalDriverContext) context).getMetadataManager();
    if (localDcFromConfig != null) {
      LOG.debug("[{}] Local DC set from configuration: {}", logPrefix, localDcFromConfig);
      this.localDc = localDcFromConfig;
    }
    this.isDefaultPolicy = isDefaultPolicy;

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
  }

  @Override
  public void init(
      @NonNull Map<InetSocketAddress, Node> nodes,
      @NonNull DistanceReporter distanceReporter,
      @NonNull Set<InetSocketAddress> contactPoints) {
    this.distanceReporter = distanceReporter;

    if (localDc == null) {
      if (contactPoints.isEmpty()) {
        // No explicit contact points provided => the driver used the default (127.0.0.1:9042), and
        // we allow inferring the local DC in this case
        Node contactPoint = nodes.get(MetadataManager.DEFAULT_CONTACT_POINT);
        localDc = contactPoint.getDatacenter();
        LOG.debug("[{}] Local DC set from contact point {}: {}", logPrefix, contactPoint, localDc);
      } else {
        throw new IllegalStateException(
            "You provided explicit contact points, the local DC must be specified (see "
                + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
                + " in the config)");
      }
    } else {
      ImmutableMap.Builder<InetSocketAddress, String> builder = ImmutableMap.builder();
      for (InetSocketAddress address : contactPoints) {
        Node node = nodes.get(address);
        if (node != null) {
          String datacenter = node.getDatacenter();
          if (!Objects.equals(localDc, datacenter)) {
            builder.put(address, (datacenter == null) ? "<null>" : datacenter);
          }
        }
      }
      ImmutableMap<InetSocketAddress, String> badContactPoints = builder.build();
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
        localDcLiveNodes.add(node);
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

  private Set<Node> getReplicas(Request request, Session session) {
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

    Optional<? extends TokenMap> maybeTokenMap = metadataManager.getMetadata().getTokenMap();
    if (maybeTokenMap.isPresent()) {
      TokenMap tokenMap = maybeTokenMap.get();
      return (token != null)
          ? tokenMap.getReplicas(keyspace, token)
          : tokenMap.getReplicas(keyspace, key);
    } else {
      return Collections.emptySet();
    }
  }

  @VisibleForTesting
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

  private static String getLocalDcFromConfig(DriverContext context, String profileName) {
    DriverExecutionProfile config = context.getConfig().getProfile(profileName);
    return config.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null);
  }

  @SuppressWarnings("unchecked")
  private static Predicate<Node> getFilterFromConfig(DriverContext context, String profileName) {
    Predicate<Node> filterFromBuilder =
        ((InternalDriverContext) context).getNodeFilter(profileName);
    return (filterFromBuilder != null)
        ? filterFromBuilder
        : (Predicate<Node>)
            Reflection.buildFromConfig(
                    (InternalDriverContext) context,
                    profileName,
                    DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS,
                    Predicate.class)
                .orElse(INCLUDE_ALL_NODES);
  }
}
