/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;

/**
 * A load balancing policy wrapper that ensure that only hosts from a provided
 * white list will ever be returned.
 * <p>
 * This policy wraps another load balancing policy and will delegate the choice
 * of hosts to the wrapped policy with the exception that only hosts contained
 * in the white list provided when constructing this policy will ever be
 * returned. Any host not in the while list will be considered {@code IGNORED}
 * and thus will not be connected to.
 * <p>
 * This policy can be useful to ensure that the driver only connects to a
 * predefined set of hosts. Keep in mind however that this policy defeats
 * somewhat the host auto-detection of the driver. As such, this policy is only
 * useful in a few special cases or for testing, but is not optimal in general.
 * If all you want to do is limiting connections to hosts of the local
 * data-center then you should use DCAwareRoundRobinPolicy and *not* this policy
 * in particular.
 */
public class WhiteListPolicy implements LoadBalancingPolicy {
    private final LoadBalancingPolicy childPolicy;
    private final Set<InetSocketAddress> whiteList;

    /**
     * Create a new policy that wraps the provided child policy but only "allow" hosts
     * from the provided while list.
     *
     * @param childPolicy the wrapped policy.
     * @param whiteList the white listed hosts. Only hosts from this list may get connected
     * to (whether they will get connected to or not depends on the child policy).
     */
    public WhiteListPolicy(LoadBalancingPolicy childPolicy, Collection<InetSocketAddress> whiteList) {
        this.childPolicy = childPolicy;
        this.whiteList = ImmutableSet.copyOf(whiteList);
    }

    /**
     * Initialize this load balancing policy.
     *
     * @param cluster the {@code Cluster} instance for which the policy is created.
     * @param hosts the initial hosts to use.
     *
     * @throws IllegalArgumentException if none of the host in {@code hosts}
     * (which will correspond to the contact points) are part of the white list.
     */
    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        List<Host> whiteHosts = new ArrayList<Host>(hosts.size());
        for (Host host : hosts)
            if (whiteList.contains(host.getSocketAddress()))
                whiteHosts.add(host);

        if (whiteHosts.isEmpty())
            throw new IllegalArgumentException(String.format("Cannot use WhiteListPolicy where the white list (%s) contains none of the contacts points (%s)", whiteList, hosts));

        childPolicy.init(cluster, whiteHosts);
    }

    /**
     * Return the HostDistance for the provided host.
     *
     * @param host the host of which to return the distance of.
     * @return {@link HostDistance#IGNORED} if {@code host} is not part of the white list, the HostDistance
     * as returned by the wrapped policy otherwise.
     */
    @Override
    public HostDistance distance(Host host) {
        return whiteList.contains(host.getSocketAddress())
             ? childPolicy.distance(host)
             : HostDistance.IGNORED;
    }

    /**
     * Returns the hosts to use for a new query.
     * <p>
     * It is guaranteed that only hosts from the white list will be returned.
     *
     * @param loggedKeyspace the currently logged keyspace (the one set through either
     * {@link Cluster#connect(String)} or by manually doing a {@code USE} query) for
     * the session on which this plan need to be built. This can be {@code null} if
     * the corresponding session has no keyspace logged in.
     * @param statement the query for which to build a plan.
     */
    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
        // Just delegate to the child policy, since we filter the hosts not white
        // listed upfront, the child policy will never see a host that is not white
        // listed and thus can't return one.
        return childPolicy.newQueryPlan(loggedKeyspace, statement);
    }

    @Override
    public void onUp(Host host) {
        if (whiteList.contains(host.getSocketAddress()))
            childPolicy.onUp(host);
    }

    @Override
    public void onSuspected(Host host) {
        if (whiteList.contains(host.getAddress()))
            childPolicy.onSuspected(host);
    }

    @Override
    public void onDown(Host host) {
        if (whiteList.contains(host.getSocketAddress()))
            childPolicy.onDown(host);
    }

    @Override
    public void onAdd(Host host) {
        if (whiteList.contains(host.getSocketAddress()))
            childPolicy.onAdd(host);
    }

    @Override
    public void onRemove(Host host) {
        if (whiteList.contains(host.getSocketAddress()))
            childPolicy.onRemove(host);
    }
}
