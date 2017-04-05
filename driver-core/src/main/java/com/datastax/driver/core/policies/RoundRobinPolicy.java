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

import com.datastax.driver.core.*;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Round-robin load balancing policy.
 * <p/>
 * This policy queries nodes in a round-robin fashion. For a given query,
 * if an host fail, the next one (following the round-robin order) is
 * tried, until all hosts have been tried.
 * <p/>
 * This policy is not datacenter aware and will include every known
 * Cassandra host in its round robin algorithm. If you use multiple
 * datacenter this will be inefficient and you will want to use the
 * {@link DCAwareRoundRobinPolicy} load balancing policy instead.
 */
public class RoundRobinPolicy implements LoadBalancingPolicy {

    private static final Logger logger = LoggerFactory.getLogger(RoundRobinPolicy.class);

    private final CopyOnWriteArrayList<Host> liveHosts = new CopyOnWriteArrayList<Host>();
    private final AtomicInteger index = new AtomicInteger();

    private volatile Configuration configuration;
    private volatile boolean hasLoggedLocalCLUse;

    /**
     * Creates a load balancing policy that picks host to query in a round robin
     * fashion (on all the hosts of the Cassandra cluster).
     */
    public RoundRobinPolicy() {
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        this.liveHosts.addAll(hosts);
        this.configuration = cluster.getConfiguration();
        this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
    }

    /**
     * Return the HostDistance for the provided host.
     * <p/>
     * This policy consider all nodes as local. This is generally the right
     * thing to do in a single datacenter deployment. If you use multiple
     * datacenter, see {@link DCAwareRoundRobinPolicy} instead.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(Host host) {
        return HostDistance.LOCAL;
    }

    /**
     * Returns the hosts to use for a new query.
     * <p/>
     * The returned plan will try each known host of the cluster. Upon each
     * call to this method, the {@code i}th host of the plans returned will cycle
     * over all the hosts of the cluster in a round-robin fashion.
     *
     * @param loggedKeyspace the keyspace currently logged in on for this
     *                       query.
     * @param statement      the query for which to build the plan.
     * @return a new query plan, i.e. an iterator indicating which host to
     * try first for querying, which one to use as failover, etc...
     */
    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {

        if (!hasLoggedLocalCLUse) {
            ConsistencyLevel cl = statement.getConsistencyLevel() == null
                    ? configuration.getQueryOptions().getConsistencyLevel()
                    : statement.getConsistencyLevel();
            if (cl.isDCLocal()) {
                hasLoggedLocalCLUse = true;
                logger.warn("Detected request at Consistency Level {} but the non-DC aware RoundRobinPolicy is in use. "
                        + "It is strongly advised to use DCAwareRoundRobinPolicy if you have multiple DCs/use DC-aware consistency levels "
                        + "(note: this message will only be logged once)", cl);
            }
        }

        // We clone liveHosts because we want a version of the list that
        // cannot change concurrently of the query plan iterator (this
        // would be racy). We use clone() as it don't involve a copy of the
        // underlying array (and thus we rely on liveHosts being a CopyOnWriteArrayList).
        @SuppressWarnings("unchecked")
        final List<Host> hosts = (List<Host>) liveHosts.clone();
        final int startIdx = index.getAndIncrement();

        // Overflow protection; not theoretically thread safe but should be good enough
        if (startIdx > Integer.MAX_VALUE - 10000)
            index.set(0);

        return new AbstractIterator<Host>() {

            private int idx = startIdx;
            private int remaining = hosts.size();

            @Override
            protected Host computeNext() {
                if (remaining <= 0)
                    return endOfData();

                remaining--;
                int c = idx++ % hosts.size();
                if (c < 0)
                    c += hosts.size();
                return hosts.get(c);
            }
        };
    }

    @Override
    public void onUp(Host host) {
        liveHosts.addIfAbsent(host);
    }

    @Override
    public void onDown(Host host) {
        liveHosts.remove(host);
    }

    @Override
    public void onAdd(Host host) {
        onUp(host);
    }

    @Override
    public void onRemove(Host host) {
        onDown(host);
    }

    @Override
    public void close() {
        // nothing to do
    }
}
