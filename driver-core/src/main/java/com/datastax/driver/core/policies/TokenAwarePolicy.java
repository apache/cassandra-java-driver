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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A wrapper load balancing policy that adds token awareness to a child policy.
 *
 * <p>This policy encapsulates another policy. The resulting policy works in the following way:
 *
 * <ul>
 *   <li>the {@code distance} method is inherited from the child policy.
 *   <li>the iterator returned by the {@code newQueryPlan} method will first return the {@link
 *       HostDistance#LOCAL LOCAL} replicas for the query <em>if possible</em> (i.e. if the query's
 *       {@linkplain Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} is not
 *       {@code null} and if the {@linkplain Metadata#getReplicas(String, ByteBuffer) set of
 *       replicas} for that partition key is not empty). If no local replica can be either found or
 *       successfully contacted, the rest of the query plan will fallback to the child policy's one.
 * </ul>
 *
 * The exact order in which local replicas are returned is dictated by the {@linkplain
 * ReplicaOrdering strategy} provided at instantiation.
 *
 * <p>Do note that only replicas for which the child policy's {@linkplain
 * LoadBalancingPolicy#distance(Host) distance} method returns {@link HostDistance#LOCAL LOCAL} will
 * be considered having priority. For example, if you wrap {@link DCAwareRoundRobinPolicy} with this
 * token aware policy, replicas from remote data centers may only be returned after all the hosts of
 * the local data center.
 */
public class TokenAwarePolicy implements ChainableLoadBalancingPolicy {

  /** Strategies for replica ordering. */
  public enum ReplicaOrdering {

    /**
     * Order replicas by token ring topology, i.e. always return the "primary" replica first, then
     * the second, etc., according to the placement of replicas around the token ring.
     *
     * <p>This strategy is the only one guaranteed to order replicas in a deterministic and constant
     * way. This increases the effectiveness of server-side row caching (especially at consistency
     * level ONE), but is more heavily impacted by hotspots, since the primary replica is always
     * tried first.
     */
    TOPOLOGICAL,

    /**
     * Return replicas in a different, random order for each query plan. This is the default
     * strategy.
     *
     * <p>This strategy fans out writes and thus can alleviate hotspots caused by "fat" partitions,
     * but its randomness makes server-side caching less efficient.
     */
    RANDOM,

    /**
     * Return the replicas in the exact same order in which they appear in the child policy's query
     * plan.
     *
     * <p>This is the only strategy that fully respects the child policy's replica ordering. Use it
     * when it is important to keep that order intact (e.g. when using the {@link
     * LatencyAwarePolicy}).
     */
    NEUTRAL
  }

  private final LoadBalancingPolicy childPolicy;
  private final ReplicaOrdering replicaOrdering;
  private volatile Metadata clusterMetadata;
  private volatile ProtocolVersion protocolVersion;
  private volatile CodecRegistry codecRegistry;

  /**
   * Creates a new {@code TokenAware} policy.
   *
   * @param childPolicy the load balancing policy to wrap with token awareness.
   * @param replicaOrdering the strategy to use to order replicas.
   */
  public TokenAwarePolicy(LoadBalancingPolicy childPolicy, ReplicaOrdering replicaOrdering) {
    this.childPolicy = childPolicy;
    this.replicaOrdering = replicaOrdering;
  }

  /**
   * Creates a new {@code TokenAware} policy.
   *
   * @param childPolicy the load balancing policy to wrap with token awareness.
   * @param shuffleReplicas whether or not to shuffle the replicas. If {@code true}, then the {@link
   *     ReplicaOrdering#RANDOM RANDOM} strategy will be used, otherwise the {@link
   *     ReplicaOrdering#TOPOLOGICAL TOPOLOGICAL} one will be used.
   * @deprecated Use {@link #TokenAwarePolicy(LoadBalancingPolicy, ReplicaOrdering)} instead. This
   *     constructor will be removed in the next major release.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  public TokenAwarePolicy(LoadBalancingPolicy childPolicy, boolean shuffleReplicas) {
    this(childPolicy, shuffleReplicas ? ReplicaOrdering.RANDOM : ReplicaOrdering.TOPOLOGICAL);
  }

  /**
   * Creates a new {@code TokenAware} policy with {@link ReplicaOrdering#RANDOM RANDOM} replica
   * ordering.
   *
   * @param childPolicy the load balancing policy to wrap with token awareness.
   */
  public TokenAwarePolicy(LoadBalancingPolicy childPolicy) {
    this(childPolicy, ReplicaOrdering.RANDOM);
  }

  @Override
  public LoadBalancingPolicy getChildPolicy() {
    return childPolicy;
  }

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    clusterMetadata = cluster.getMetadata();
    protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    codecRegistry = cluster.getConfiguration().getCodecRegistry();
    childPolicy.init(cluster, hosts);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation always returns distances as reported by the wrapped policy.
   */
  @Override
  public HostDistance distance(Host host) {
    return childPolicy.distance(host);
  }

  /**
   * {@inheritDoc}
   *
   * <p>The returned plan will first return local replicas for the query (i.e. replicas whose
   * {@linkplain HostDistance distance} according to the child policy is {@code LOCAL}), if it can
   * determine them (i.e. mainly if the statement's {@linkplain
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} is not {@code null}), and
   * ordered according to the {@linkplain ReplicaOrdering ordering strategy} specified at
   * instantiation; following what it will return the rest of the child policy's original query
   * plan.
   */
  @Override
  public Iterator<Host> newQueryPlan(final String loggedKeyspace, final Statement statement) {

    ByteBuffer partitionKey = statement.getRoutingKey(protocolVersion, codecRegistry);
    String keyspace = statement.getKeyspace();
    if (keyspace == null) keyspace = loggedKeyspace;

    if (partitionKey == null || keyspace == null)
      return childPolicy.newQueryPlan(keyspace, statement);

    final Set<Host> replicas = clusterMetadata.getReplicas(Metadata.quote(keyspace), partitionKey);
    if (replicas.isEmpty()) return childPolicy.newQueryPlan(loggedKeyspace, statement);

    if (replicaOrdering == ReplicaOrdering.NEUTRAL) {

      final Iterator<Host> childIterator = childPolicy.newQueryPlan(keyspace, statement);

      return new AbstractIterator<Host>() {

        private List<Host> nonReplicas;
        private Iterator<Host> nonReplicasIterator;

        @Override
        protected Host computeNext() {

          while (childIterator.hasNext()) {

            Host host = childIterator.next();

            if (host.isUp()
                && replicas.contains(host)
                && childPolicy.distance(host) == HostDistance.LOCAL) {
              // UP replicas should be prioritized, retaining order from childPolicy
              return host;
            } else {
              // save for later
              if (nonReplicas == null) nonReplicas = new ArrayList<Host>();
              nonReplicas.add(host);
            }
          }

          // This should only engage if all local replicas are DOWN
          if (nonReplicas != null) {

            if (nonReplicasIterator == null) nonReplicasIterator = nonReplicas.iterator();

            if (nonReplicasIterator.hasNext()) return nonReplicasIterator.next();
          }

          return endOfData();
        }
      };

    } else {

      final Iterator<Host> replicasIterator;

      if (replicaOrdering == ReplicaOrdering.RANDOM) {
        List<Host> replicasList = Lists.newArrayList(replicas);
        Collections.shuffle(replicasList);
        replicasIterator = replicasList.iterator();
      } else {
        replicasIterator = replicas.iterator();
      }

      return new AbstractIterator<Host>() {

        private Iterator<Host> childIterator;

        @Override
        protected Host computeNext() {
          while (replicasIterator.hasNext()) {
            Host host = replicasIterator.next();
            if (host.isUp() && childPolicy.distance(host) == HostDistance.LOCAL) return host;
          }

          if (childIterator == null)
            childIterator = childPolicy.newQueryPlan(loggedKeyspace, statement);

          while (childIterator.hasNext()) {
            Host host = childIterator.next();
            // Skip it if it was already a local replica
            if (!replicas.contains(host) || childPolicy.distance(host) != HostDistance.LOCAL)
              return host;
          }
          return endOfData();
        }
      };
    }
  }

  @Override
  public void onUp(Host host) {
    childPolicy.onUp(host);
  }

  @Override
  public void onDown(Host host) {
    childPolicy.onDown(host);
  }

  @Override
  public void onAdd(Host host) {
    childPolicy.onAdd(host);
  }

  @Override
  public void onRemove(Host host) {
    childPolicy.onRemove(host);
  }

  @Override
  public void close() {
    childPolicy.close();
  }
}
