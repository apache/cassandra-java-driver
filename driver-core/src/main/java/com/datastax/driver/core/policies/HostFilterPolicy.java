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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.base.Predicate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;

/**
 * A load balancing policy wrapper that ensure that only hosts matching the predicate
 * will ever be returned.
 * <p>
 * This policy wraps another load balancing policy and will delegate the choice
 * of hosts to the wrapped policy with the exception that only hosts matching 
 * the predicate provided when constructing this policy will ever be
 * returned. Any host not matching the predicate will be considered {@code IGNORED}
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
public class HostFilterPolicy implements ChainableLoadBalancingPolicy, CloseableLoadBalancingPolicy {
    private final LoadBalancingPolicy childPolicy;
    private final Predicate<Host> predicate;

    /**
     * Create a new policy that wraps the provided child policy but only "allow" hosts
     * matching the predicate.
     *
     * @param childPolicy the wrapped policy.
     * @param predicate the host predicate. Only hosts matching this predicate may get connected
     * to (whether they will get connected to or not depends on the child policy).
     */
    public HostFilterPolicy(LoadBalancingPolicy childPolicy, Predicate<Host> predicate) {
        this.childPolicy = childPolicy;
        this.predicate = predicate;
    }

    @Override
    public LoadBalancingPolicy getChildPolicy() {
        return childPolicy;
    }

    /**
     * Initialize this load balancing policy.
     *
     * @param cluster the {@code Cluster} instance for which the policy is created.
     * @param hosts the initial hosts to use.
     *
     * @throws IllegalArgumentException if none of the host in {@code hosts}
     * (which will correspond to the contact points) matches the predicate.
     */
    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        List<Host> whiteHosts = new ArrayList<Host>(hosts.size());
        for (Host host : hosts)
            if (predicate.apply(host))
                whiteHosts.add(host);

        if (whiteHosts.isEmpty())
            throw new IllegalArgumentException(String.format("Cannot use HostFilterPolicy where the filter allows none of the contacts points (%s)", hosts));

        childPolicy.init(cluster, whiteHosts);
    }

    /**
     * Return the HostDistance for the provided host.
     *
     * @param host the host of which to return the distance of.
     * @return {@link HostDistance#IGNORED} if {@code host} is not matching the predicate, the HostDistance
     * as returned by the wrapped policy otherwise.
     */
    @Override
    public HostDistance distance(Host host) {
        return predicate.apply(host)
             ? childPolicy.distance(host)
             : HostDistance.IGNORED;
    }

    /**
     * Returns the hosts to use for a new query.
     * <p>
     * It is guaranteed that only hosts matching the predicate will be returned.
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
        if (predicate.apply(host))
            childPolicy.onUp(host);
    }

    @Override
    public void onSuspected(Host host) {
        if (predicate.apply(host))
            childPolicy.onSuspected(host);
    }

    @Override
    public void onDown(Host host) {
        if (predicate.apply(host))
            childPolicy.onDown(host);
    }

    @Override
    public void onAdd(Host host) {
        if (predicate.apply(host))
            childPolicy.onAdd(host);
    }

    @Override
    public void onRemove(Host host) {
        if (predicate.apply(host))
            childPolicy.onRemove(host);
    }

    @Override
    public void close() {
        if (childPolicy instanceof CloseableLoadBalancingPolicy)
            ((CloseableLoadBalancingPolicy)childPolicy).close();
    }
    
    /**
     * Create a new policy that wraps the provided child policy but only "allow/forbids" hosts
     * from the provided list.
     *
     * @param childPolicy the wrapped policy.
     * @param hosts the hosts.
     * @param allow when true allow hosts from the provided list to be connected to 
     * (whether they will get connected to or not depends on the child policy), otherwise forbids these hosts
     */
    public static HostFilterPolicy mkHostAddressListPolicy( LoadBalancingPolicy childPolicy, Collection<InetSocketAddress> hosts, boolean allow) {
        Predicate<Host> predicate = mkHostAddressPredicate(hosts);
        return new HostFilterPolicy(childPolicy, allow ? predicate : Predicates.not(predicate));
    }
    
    /**
     * Create a new policy that wraps the provided child policy but only "allow/forbids" hosts
     * whose DC belongs to the provided list.
     *
     * @param childPolicy the wrapped policy.
     * @param dcs the DCs.
     * @param allow when true allow hosts whose DC from the provided list to be connected to 
     * (whether they will get connected to or not depends on the child policy), otherwise forbids these hosts
     */
    public static HostFilterPolicy mkDCListPolicy( LoadBalancingPolicy childPolicy, Collection<String> dcs, boolean allow) {
        Predicate<Host> predicate = mkHostDCPredicate(dcs, allow);
        return new HostFilterPolicy(childPolicy, allow ? predicate : Predicates.not(predicate));
    }
    
    /**
     * Create a predicate based on membership to a list of addresses
     * @param hosts the host addresses
     * @return the predicate
     */
    public static Predicate<Host> mkHostAddressPredicate( Collection<InetSocketAddress> hosts) {
        final ImmutableSet<InetSocketAddress> _hosts = ImmutableSet.copyOf(hosts);
        return new Predicate<Host>() {
            @Override
            public boolean apply(Host host) {
                return _hosts.contains(host.getSocketAddress());
            }
        };
    }
    
    /**
     * Create a predicate based on membership to a list of DCs
     * @param dcs the data centers
     * @param onNull the predicate value when host's DC is undefined
     * @return the predicate
     */
    public static Predicate<Host> mkHostDCPredicate( Collection<String> dcs, final boolean onNull) {
        final ImmutableSet<String> _dcs = ImmutableSet.copyOf(dcs);
        return new Predicate<Host>() {
            @Override
            public boolean apply(Host host) {
                String hdc = host.getDatacenter();
                return (hdc == null) ? onNull : _dcs.contains(hdc);
            }
        };
    }

}
