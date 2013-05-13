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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.AbstractIterator;

import com.datastax.driver.core.*;

/**
 * A data-center aware Round-robin load balancing policy.
 * <p>
 * This policy provides round-robin queries over the node of the local
 * datacenter. It also includes in the query plans returned a configurable
 * number of hosts in the remote datacenters, but those are always tried
 * after the local nodes. In other words, this policy guarantees that no
 * host in a remote datacenter will be queried unless no host in the local
 * datacenter can be reached.
 * <p>
 * If used with a single datacenter, this policy is equivalent to the
 * {@code LoadBalancingPolicy.RoundRobin} policy, but its DC awareness
 * incurs a slight overhead so the {@code LoadBalancingPolicy.RoundRobin}
 * policy could be prefered to this policy in that case.
 */
public class DCAwareRoundRobinPolicy implements LoadBalancingPolicy {

    private final ConcurrentMap<String, CopyOnWriteArrayList<Host>> perDcLiveHosts = new ConcurrentHashMap<String, CopyOnWriteArrayList<Host>>();
    private final AtomicInteger index = new AtomicInteger();
    private final String localDc;
    private final int usedHostsPerRemoteDc;

    /**
     * Creates a new datacenter aware round robin policy given the name of
     * the local datacenter.
     * <p>
     * The name of the local datacenter provided must be the local
     * datacenter name as known by Cassandra.
     * <p>
     * The policy created will ignore all remote hosts. In other words,
     * this is equivalent to {@code new DCAwareRoundRobinPolicy(localDc, 0)}.
     *
     * @param localDc the name of the local datacenter (as known by
     * Cassandra).
     */
    public DCAwareRoundRobinPolicy(String localDc) {
        this(localDc, 0);
    }

    /**
     * Creates a new DCAwareRoundRobin policy given the name of the local
     * datacenter and that uses the provided number of host per remote
     * datacenter as failover for the local hosts.
     * <p>
     * The name of the local datacenter provided must be the local
     * datacenter name as known by Cassandra.
     *
     * @param localDc the name of the local datacenter (as known by
     * Cassandra).
     * @param usedHostsPerRemoteDc the number of host per remote
     * datacenter that policies created by the returned factory should
     * consider. Created policies {@code distance} method will return a
     * {@code HostDistance.REMOTE} distance for only {@code
     * usedHostsPerRemoteDc} hosts per remote datacenter. Other hosts
     * of the remote datacenters will be ignored (and thus no
     * connections to them will be maintained).
     */
    public DCAwareRoundRobinPolicy(String localDc, int usedHostsPerRemoteDc) {
        this.localDc = localDc;
        this.usedHostsPerRemoteDc = usedHostsPerRemoteDc;
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));

        for (Host host : hosts) {
            String dc = dc(host);
            CopyOnWriteArrayList<Host> prev = perDcLiveHosts.get(dc);
            if (prev == null)
                perDcLiveHosts.put(dc, new CopyOnWriteArrayList<Host>(Collections.singletonList(host)));
            else
                prev.addIfAbsent(host);
        }
    }

    private String dc(Host host) {
        String dc = host.getDatacenter();
        return dc == null ? localDc : dc;
    }

    @SuppressWarnings("unchecked")
    private static CopyOnWriteArrayList<Host> cloneList(CopyOnWriteArrayList<Host> list) {
        return (CopyOnWriteArrayList<Host>)list.clone();
    }

    /**
     * Return the HostDistance for the provided host.
     * <p>
     * This policy consider nodes in the local datacenter as {@code LOCAL}.
     * For each remote datacenter, it considers a configurable number of
     * hosts as {@code REMOTE} and the rest is {@code IGNORED}.
     * <p>
     * To configure how many host in each remote datacenter is considered
     * {@code REMOTE}, see {@link #DCAwareRoundRobinPolicy(String, int)}.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(Host host) {
        String dc = dc(host);
        if (dc.equals(localDc))
            return HostDistance.LOCAL;

        CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc);
        if (dcHosts == null || usedHostsPerRemoteDc == 0)
            return HostDistance.IGNORED;

        // We need to clone, otherwise our subList call is not thread safe
        dcHosts = cloneList(dcHosts);
        return dcHosts.subList(0, Math.min(dcHosts.size(), usedHostsPerRemoteDc)).contains(host)
             ? HostDistance.REMOTE
             : HostDistance.IGNORED;
    }

    /**
     * Returns the hosts to use for a new query.
     * <p>
     * The returned plan will always try each known host in the local
     * datacenter first, and then, if none of the local host is reachable,
     * will try up to a configurable number of other host per remote datacenter.
     * The order of the local node in the returned query plan will follow a
     * Round-robin algorithm.
     *
     * @param query the query for which to build the plan.
     * @return a new query plan, i.e. an iterator indicating which host to
     * try first for querying, which one to use as failover, etc...
     */
    @Override
    public Iterator<Host> newQueryPlan(Query query) {

        CopyOnWriteArrayList<Host> localLiveHosts = perDcLiveHosts.get(localDc);
        final List<Host> hosts = localLiveHosts == null ? Collections.<Host>emptyList() : cloneList(localLiveHosts);
        final int startIdx = index.getAndIncrement();

        // Overflow protection; not theoretically thread safe but should be good enough
        if (startIdx > Integer.MAX_VALUE - 10000)
            index.set(0);

        return new AbstractIterator<Host>() {

            private int idx = startIdx;
            private int remainingLocal = hosts.size();

            // For remote Dcs
            private Iterator<String> remoteDcs;
            private List<Host> currentDcHosts;
            private int currentDcRemaining;

            @Override
            protected Host computeNext() {
                if (remainingLocal > 0) {
                    remainingLocal--;
                    return hosts.get(idx++ % hosts.size());
                }

                if (currentDcHosts != null && currentDcRemaining > 0) {
                    currentDcRemaining--;
                    return currentDcHosts.get(idx++ % currentDcHosts.size());
                }

                if (remoteDcs == null) {
                    Set<String> copy = new HashSet<String>(perDcLiveHosts.keySet());
                    copy.remove(localDc);
                    remoteDcs = copy.iterator();
                }

                if (!remoteDcs.hasNext())
                    return endOfData();

                String nextRemoteDc = remoteDcs.next();
                CopyOnWriteArrayList<Host> nextDcHosts = perDcLiveHosts.get(nextRemoteDc);
                if (nextDcHosts != null) {
                    // Clone for thread safety
                    List<Host> dcHosts = cloneList(nextDcHosts);
                    currentDcHosts = dcHosts.subList(0, Math.min(dcHosts.size(), usedHostsPerRemoteDc));
                    currentDcRemaining = currentDcHosts.size();
                }

                return computeNext();
            }
        };
    }

    @Override
    public void onUp(Host host) {
        String dc = dc(host);
        CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc);
        if (dcHosts == null) {
            CopyOnWriteArrayList<Host> newMap = new CopyOnWriteArrayList<Host>(Collections.singletonList(host));
            dcHosts = perDcLiveHosts.putIfAbsent(dc, newMap);
            // If we've successfully put our new host, we're good, otherwise we've been beaten so continue
            if (dcHosts == null)
                return;
        }
        dcHosts.addIfAbsent(host);
    }

    @Override
    public void onDown(Host host) {
        CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc(host));
        if (dcHosts != null)
            dcHosts.remove(host);
    }

    @Override
    public void onAdd(Host host) {
        onUp(host);
    }

    @Override
    public void onRemove(Host host) {
        onDown(host);
    }
}
