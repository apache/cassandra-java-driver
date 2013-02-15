/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.AbstractIterator;

import com.datastax.driver.core.*;

/**
 * A wrapper load balancing policy that add token awareness to a child policy.
 * <p>
 * This policy encapsulates another policy. The resulting policy works in
 * the following way:
 * <ul>
 *   <li>the {@code distance} method is inherited from the child policy.</li>
 *   <li>the iterator return by the {@code newQueryPlan} method will first
 *   return the {@code LOCAL} replicas for the query (based on {@link Query#getRoutingKey})
 *   <i>if possible</i> (i.e. if the query {@code getRoutingKey} method
 *   doesn't return {@code null} and if {@link Metadata#getReplicas}
 *   returns a non empty set of replicas for that partition key). If no
 *   local replica can be either found or successfully contacted, the rest
 *   of the query plan will fallback to one of the child policy.</li>
 * </ul>
 * <p>
 * Do note that only replica for which the child policy {@code distance}
 * method returns {@code HostDistance.LOCAL} will be considered having
 * priority. For example, if you wrap {@link DCAwareRoundRobinPolicy} with this
 * token aware policy, replicas from remote data centers may only be
 * returned after all the host of the local data center.
 */
public class TokenAwarePolicy implements LoadBalancingPolicy {

    private final LoadBalancingPolicy childPolicy;
    private Metadata clusterMetadata;

    /**
     * Creates a new {@code TokenAware} policy that wraps the provided child
     * load balancing policy.
     *
     * @param childPolicy the load balancing policy to wrap with token
     * awareness.
     */
    public TokenAwarePolicy(LoadBalancingPolicy childPolicy) {
        this.childPolicy = childPolicy;
    }

    public void init(Cluster cluster, Collection<Host> hosts) {
        clusterMetadata = cluster.getMetadata();
        childPolicy.init(cluster, hosts);
    }

    /**
     * Return the HostDistance for the provided host.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host} as returned by the wrapped policy.
     */
    public HostDistance distance(Host host) {
        return childPolicy.distance(host);
    }

    /**
     * Returns the hosts to use for a new query.
     * <p>
     * The returned plan will first return replicas (whose {@code HostDistance}
     * for the child policy is {@code LOCAL}) for the query if it can determine
     * them (i.e. mainly if {@code query.getRoutingKey()} is not {@code null}).
     * Following what it will return the plan of the child policy.
     *
     * @param query the query for which to build the plan.
     * @return the new query plan.
     */
    public Iterator<Host> newQueryPlan(final Query query) {

        ByteBuffer partitionKey = query.getRoutingKey();
        if (partitionKey == null)
            return childPolicy.newQueryPlan(query);

        final Set<Host> replicas = clusterMetadata.getReplicas(partitionKey);
        if (replicas.isEmpty())
            return childPolicy.newQueryPlan(query);

        return new AbstractIterator<Host>() {

            private final Iterator<Host> iter = replicas.iterator();
            private Iterator<Host> childIterator;

            protected Host computeNext() {
                while (iter.hasNext()) {
                    Host host = iter.next();
                    if (host.getMonitor().isUp() && childPolicy.distance(host) == HostDistance.LOCAL)
                        return host;
                }

                if (childIterator == null)
                    childIterator = childPolicy.newQueryPlan(query);

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

    public void onUp(Host host) {
        childPolicy.onUp(host);
    }

    public void onDown(Host host) {
        childPolicy.onDown(host);
    }

    public void onAdd(Host host) {
        childPolicy.onAdd(host);
    }

    public void onRemove(Host host) {
        childPolicy.onRemove(host);
    }
}
