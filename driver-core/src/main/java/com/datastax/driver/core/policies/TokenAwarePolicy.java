/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.policies;

import com.datastax.driver.core.*;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A wrapper load balancing policy that adds token awareness to a child policy.
 * <p/>
 * This policy encapsulates another policy. The resulting policy works in
 * the following way:
 * <ul>
 * <li>the {@code distance} method is inherited from the child policy.</li>
 * <li>the iterator returned by the {@code newQueryPlan} method will first
 * return the {@link HostDistance#LOCAL LOCAL} replicas for the query
 * <em>if possible</em> (i.e. if the query's
 * {@link Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}
 * is not {@code null} and if the
 * {@link Metadata#getReplicas(String, ByteBuffer) set of replicas}
 * for that partition key is not empty). If no local replica can be either found
 * or successfully contacted, the rest of the query plan will fallback
 * to the child policy's one.</li>
 * </ul>
 * <p/>
 * Do note that only replicas for which the child policy's
 * {@link LoadBalancingPolicy#distance(Host) distance}
 * method returns {@link HostDistance#LOCAL LOCAL} will be considered having
 * priority. For example, if you wrap {@link DCAwareRoundRobinPolicy} with this
 * token aware policy, replicas from remote data centers may only be
 * returned after all the hosts of the local data center.
 */
public class TokenAwarePolicy implements ChainableLoadBalancingPolicy {

    private final LoadBalancingPolicy childPolicy;
    private volatile Metadata clusterMetadata;
    private volatile ProtocolVersion protocolVersion;
    private volatile CodecRegistry codecRegistry;

    /**
     * Creates a new {@code TokenAware} policy.
     *
     * @param childPolicy     the load balancing policy to wrap with token awareness.
     * @param shuffleReplicas ignored.
     * @deprecated The parameter {@code shuffleReplicas} has been deprecated and has no effect anymore.
     * Use {@link #TokenAwarePolicy(LoadBalancingPolicy)} instead. This constructor will be removed
     * in the next major release.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public TokenAwarePolicy(LoadBalancingPolicy childPolicy, @SuppressWarnings("unused") boolean shuffleReplicas) {
        this.childPolicy = childPolicy;
    }

    /**
     * Creates a new {@code TokenAware} policy.
     *
     * @param childPolicy the load balancing policy to wrap with token awareness.
     */
    public TokenAwarePolicy(LoadBalancingPolicy childPolicy) {
        this.childPolicy = childPolicy;
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
     * <p/>
     * This implementation always returns distances as reported by the wrapped policy.
     */
    @Override
    public HostDistance distance(Host host) {
        return childPolicy.distance(host);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * The returned plan will first return local replicas for the query (i.e.
     * replicas whose {@link HostDistance distance} according to the child policy is {@code LOCAL}),
     * if it can determine them (i.e. mainly if the statement's
     * {@link Statement#getRoutingKey(ProtocolVersion, CodecRegistry)} routing key}
     * is not {@code null}); following what it will return the child policy's original query plan.
     */
    @Override
    public Iterator<Host> newQueryPlan(final String loggedKeyspace, final Statement statement) {

        ByteBuffer partitionKey = statement.getRoutingKey(protocolVersion, codecRegistry);
        String keyspace = statement.getKeyspace();
        if (keyspace == null)
            keyspace = loggedKeyspace;

        if (partitionKey == null || keyspace == null)
            return childPolicy.newQueryPlan(keyspace, statement);

        Set<Host> replicas = clusterMetadata.getReplicas(Metadata.quote(keyspace), partitionKey);

        Iterator<Host> childIterator = childPolicy.newQueryPlan(loggedKeyspace, statement);

        if (replicas.isEmpty())
            return childIterator;

        List<Host> queryPlan = new ArrayList<Host>();
        int i = 0;
        while (childIterator.hasNext()) {
            Host host = childIterator.next();
            if (host.isUp() && childPolicy.distance(host) == HostDistance.LOCAL && replicas.contains(host))
                queryPlan.add(i++, host);
            else
                queryPlan.add(host);
        }
        return queryPlan.iterator();

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
