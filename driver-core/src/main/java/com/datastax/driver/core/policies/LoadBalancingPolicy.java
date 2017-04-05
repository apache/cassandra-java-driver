/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;

import java.util.Collection;
import java.util.Iterator;

/**
 * The policy that decides which Cassandra hosts to contact for each new query.
 * <p/>
 * Two methods need to be implemented:
 * <ul>
 * <li>{@link LoadBalancingPolicy#distance}: returns the "distance" of an
 * host for that balancing policy. </li>
 * <li>{@link LoadBalancingPolicy#newQueryPlan}: it is used for each query to
 * find which host to query first, and which hosts to use as failover.</li>
 * </ul>
 * <p/>
 * The {@code LoadBalancingPolicy} is informed of hosts up/down events. For efficiency
 * purposes, the policy is expected to exclude down hosts from query plans.
 */
public interface LoadBalancingPolicy {

    /**
     * Initialize this load balancing policy.
     * <p/>
     * Note that the driver guarantees that it will call this method exactly
     * once per policy object and will do so before any call to another of the
     * methods of the policy.
     *
     * @param cluster the {@code Cluster} instance for which the policy is created.
     * @param hosts   the initial hosts to use.
     */
    public void init(Cluster cluster, Collection<Host> hosts);

    /**
     * Returns the distance assigned by this policy to the provided host.
     * <p/>
     * The distance of an host influence how much connections are kept to the
     * node (see {@link HostDistance}). A policy should assign a {@code
     * LOCAL} distance to nodes that are susceptible to be returned first by
     * {@code newQueryPlan} and it is useless for {@code newQueryPlan} to
     * return hosts to which it assigns an {@code IGNORED} distance.
     * <p/>
     * The host distance is primarily used to prevent keeping too many
     * connections to host in remote datacenters when the policy itself always
     * picks host in the local datacenter first.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    public HostDistance distance(Host host);

    /**
     * Returns the hosts to use for a new query.
     * <p/>
     * Each new query will call this method. The first host in the result will
     * then be used to perform the query. In the event of a connection problem
     * (the queried host is down or appear to be so), the next host will be
     * used. If all hosts of the returned {@code Iterator} are down, the query
     * will fail.
     *
     * @param loggedKeyspace the currently logged keyspace (the one set through either
     *                       {@link Cluster#connect(String)} or by manually doing a {@code USE} query) for
     *                       the session on which this plan need to be built. This can be {@code null} if
     *                       the corresponding session has no keyspace logged in.
     * @param statement      the query for which to build a plan.
     * @return an iterator of Host. The query is tried against the hosts
     * returned by this iterator in order, until the query has been sent
     * successfully to one of the host.
     */
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement);

    /**
     * Called when a new node is added to the cluster.
     * <p/>
     * The newly added node should be considered up.
     *
     * @param host the host that has been newly added.
     */
    void onAdd(Host host);

    /**
     * Called when a node is determined to be up.
     *
     * @param host the host that has been detected up.
     */
    void onUp(Host host);

    /**
     * Called when a node is determined to be down.
     *
     * @param host the host that has been detected down.
     */
    void onDown(Host host);

    /**
     * Called when a node is removed from the cluster.
     *
     * @param host the removed host.
     */
    void onRemove(Host host);

    /**
     * Gets invoked at cluster shutdown.
     * <p/>
     * This gives the policy the opportunity to perform some cleanup, for instance stop threads that it might have started.
     */
    void close();
}
