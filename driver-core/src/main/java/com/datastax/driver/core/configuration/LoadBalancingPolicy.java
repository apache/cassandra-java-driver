package com.datastax.driver.core.configuration;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.AbstractIterator;

import com.datastax.driver.core.*;

/**
 * The policy that decides which Cassandra hosts to contact for each new query.
 * <p>
 * Two methods need to be implemented:
 * <ul>
 *   <li>{@link LoadBalancingPolicy#distance}: returns the "distance" of an
 *   host for that balancing policy. </li>
 *   <li>{@link LoadBalancingPolicy#newQueryPlan}: it is used for each query to
 *   find which host to query first, and which hosts to use as failover.</li>
 * </ul>
 * <p>
 * The {@code LoadBalancingPolicy} is a {@link Host.StateListener} and is thus
 * informed of hosts up/down events. For efficiency purposes, the policy is
 * expected to exclude down hosts from query plans.
 */
public interface LoadBalancingPolicy extends Host.StateListener {

    /**
     * Returns the distance assigned by this policy to the provided host.
     * <p>
     * The distance of an host influence how much connections are kept to the
     * node (see {@link HostDistance}). A policy should assign a {@code
     * LOCAL} distance to nodes that are susceptible to be returned first by
     * {@code newQueryPlan} and it is useless for {@code newQueryPlan} to
     * return hosts to which it assigns an {@code IGNORED} distance.
     * <p>
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
     * <p>
     * Each new query will call this method. The first host in the result will
     * then be used to perform the query. In the event of a connection problem
     * (the queried host is down or appear to be so), the next host will be
     * used. If all hosts of the returned {@code Iterator} are down, the query
     * will fail.
     *
     * @param queryOptions the options used for the query.
     * @return an iterator of Host. The query is tried against the hosts
     * returned by this iterator in order, until the query has been sent
     * successfully to one of the host.
     */
    public Iterator<Host> newQueryPlan(QueryOptions queryOptions);

    /**
     * Simple factory interface to allow creating {@link LoadBalancingPolicy} instances.
     */
    public interface Factory {

        /**
         * Creates a new LoadBalancingPolicy instance over the provided (initial) {@code hosts}.
         *
         * @param hosts the initial hosts to use.
         * @return the newly created {@link LoadBalancingPolicy} instance.
         */
        public LoadBalancingPolicy create(Collection<Host> hosts);
    }

    /**
     * A Round-robin load balancing policy.
     * <p>
     * This policy queries nodes in a round-robin fashion. For a given query,
     * if an host fail, the next one (following the round-robin order) is
     * tried, until all hosts have been tried.
     * <p>
     * This policy is not datacenter aware and will include every known
     * Cassandra host in its round robin algorithm. If you use multiple
     * datacenter this will be inefficient and you will want to use the
     * DCAwareRoundRobin load balancing policy instead.
     */
    public static class RoundRobin implements LoadBalancingPolicy {

        private final CopyOnWriteArrayList<Host> liveHosts;
        private final AtomicInteger index = new AtomicInteger();

        private RoundRobin(Collection<Host> hosts) {
            this.liveHosts = new CopyOnWriteArrayList<Host>(hosts);
            this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
        }

        /**
         * Return the HostDistance for the provided host.
         * <p>
         * This policy consider all nodes as local. This is generally the right
         * thing to do in a single datacenter deployement. If you use multiple
         * datacenter, see {@link DCAwareRoundRobin} instead.
         *
         * @param host the host of which to return the distance of.
         * @return the HostDistance to {@code host}.
         */
        public HostDistance distance(Host host) {
            return HostDistance.LOCAL;
        }

        /**
         * Returns the hosts to use for a new query.
         * <p>
         * The returned plan will try each known host of the cluster. Upon each
         * call to this method, the ith host of the plans returned will cycle
         * over all the host of the cluster in a round-robin fashion.
         *
         * @return a new query plan, i.e. an iterator indicating which host to
         * try first for querying, which one to use as failover, etc...
         */
        public Iterator<Host> newQueryPlan(QueryOptions queryOptions) {

            // We clone liveHosts because we want a version of the list that
            // cannot change concurrently of the query plan iterator (this
            // would be racy). We use clone() as it don't involve a copy of the
            // underlying array (and thus we rely on liveHosts being a CopyOnWriteArrayList).
            final List<Host> hosts = (List<Host>)liveHosts.clone();
            final int startIdx = index.getAndIncrement();

            // Overflow protection; not theoretically thread safe but should be good enough
            if (startIdx > Integer.MAX_VALUE - 10000)
                index.set(0);

            return new AbstractIterator<Host>() {

                private int idx = startIdx;
                private int remaining = hosts.size();

                protected Host computeNext() {
                    if (remaining <= 0)
                        return endOfData();

                    remaining--;
                    return hosts.get(idx++ % hosts.size());
                }
            };
        }

        public void onUp(Host host) {
            liveHosts.addIfAbsent(host);
        }

        public void onDown(Host host) {
            liveHosts.remove(host);
        }

        public void onAdd(Host host) {
            onUp(host);
        }

        public void onRemove(Host host) {
            onDown(host);
        }

        /**
         * A {@code LoadBalancingPolicy.Factory} that creates RoundRobin
         * policies (on the whole cluster).
         */
        public static class Factory implements LoadBalancingPolicy.Factory {

            public static final Factory INSTANCE = new Factory();

            private Factory() {}

            public LoadBalancingPolicy create(Collection<Host> hosts) {
                return new RoundRobin(hosts);
            }
        }
    }

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
    public static class DCAwareRoundRobin implements LoadBalancingPolicy {

        private final ConcurrentMap<String, CopyOnWriteArrayList<Host>> perDcLiveHosts = new ConcurrentHashMap<String, CopyOnWriteArrayList<Host>>();
        private final AtomicInteger index = new AtomicInteger();
        private final String localDc;
        private final int usedHostsPerRemoteDc;

        private DCAwareRoundRobin(Collection<Host> hosts, String localDc, int usedHostsPerRemoteDc) {
            this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
            this.localDc = localDc;
            this.usedHostsPerRemoteDc = usedHostsPerRemoteDc;

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

        /**
         * Return the HostDistance for the provided host.
         * <p>
         * This policy consider nodes in the local datacenter as {@code LOCAL}.
         * For each remote datacenter, it considers a configurable number of
         * hosts as {@code REMOTE} and the rest is {@code IGNORED}.
         * <p>
         * To configure how many host in each remote datacenter is considered
         * {@code REMOTE}, see {@link Factory#create(String, int)}.
         *
         * @param host the host of which to return the distance of.
         * @return the HostDistance to {@code host}.
         */
        public HostDistance distance(Host host) {
            String dc = dc(host);
            if (dc.equals(localDc))
                return HostDistance.LOCAL;

            CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc);
            if (dcHosts == null || usedHostsPerRemoteDc == 0)
                return HostDistance.IGNORED;

            // We need to clone, otherwise our subList call is not thread safe
            dcHosts = (CopyOnWriteArrayList<Host>)dcHosts.clone();
            return dcHosts.subList(0, Math.min(dcHosts.size(), usedHostsPerRemoteDc)).contains(host)
                 ? HostDistance.REMOTE
                 : HostDistance.IGNORED;
        }

        /**
         * Returns the hosts to use for a new query.
         * <p>
         * The returned plan will always try each known host in the local
         * datacenter first, and then, if none of the local host is reacheable,
         * will try up to a configurable number of other host per remote datacenter.
         * The order of the local node in the returned query plan will follow a
         * Round-robin algorithm.
         *
         * @return a new query plan, i.e. an iterator indicating which host to
         * try first for querying, which one to use as failover, etc...
         */
        public Iterator<Host> newQueryPlan(QueryOptions queryOptions) {

            CopyOnWriteArrayList<Host> localLiveHosts = perDcLiveHosts.get(localDc);
            final List<Host> hosts = localLiveHosts == null ? Collections.<Host>emptyList() : (List<Host>)localLiveHosts.clone();
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
                        Set<String> copy = new HashSet(perDcLiveHosts.keySet());
                        copy.remove(localDc);
                        remoteDcs = copy.iterator();
                    }

                    if (!remoteDcs.hasNext())
                        return endOfData();

                    String nextRemoteDc = remoteDcs.next();
                    CopyOnWriteArrayList<Host> nextDcHosts = perDcLiveHosts.get(nextRemoteDc);
                    if (nextDcHosts != null) {
                        currentDcHosts = (List<Host>)nextDcHosts.clone();
                        currentDcRemaining = Math.min(usedHostsPerRemoteDc, currentDcHosts.size());
                    }

                    return computeNext();
                }
            };
        }

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

        public void onDown(Host host) {
            CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc(host));
            if (dcHosts != null)
                dcHosts.remove(host);
        }

        public void onAdd(Host host) {
            onUp(host);
        }

        public void onRemove(Host host) {
            onDown(host);
        }

        /**
         * A {@code LoadBalancingPolicy.Factory} that creates DCAwareRoundRobin
         * policies.
         */
        public static class Factory implements LoadBalancingPolicy.Factory {

            public static final int DEFAULT_USED_HOSTS_PER_REMOTE_DC = 0;

            private final String localDc;
            private final int usedHostsPerRemoteDc;

            private Factory(String localDc, int usedHostsPerRemoteDc) {
                this.localDc = localDc;
                this.usedHostsPerRemoteDc = usedHostsPerRemoteDc;
            }

            /**
             * Creates a new DCAwareRoundRobin policy factory given the name of
             * the local datacenter.
             *
             * The name of the local datacenter provided must be the local
             * datacenter name as known by Cassandra.
             *
             * The policy created by the returned factory will ignore all
             * remote hosts. In other words, this is equivalent to
             * {@code create(localDc, 0)}.
             *
             * @param localDc the name of the local datacenter (as known by
             * Cassandra).
             * @return the newly created factory.
             */
            public static Factory create(String localDc) {
                return new Factory(localDc, DEFAULT_USED_HOSTS_PER_REMOTE_DC);
            }

            /**
             * Creates a new DCAwareRoundRobin policy factory given the name of
             * the local datacenter that use the provided number of host per
             * remote datacenter as failover for the local hosts.
             *
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
             * @return the newly created factory.
             */
            public static Factory create(String localDc, int usedHostsPerRemoteDc) {
                return new Factory(localDc, usedHostsPerRemoteDc);
            }

            public LoadBalancingPolicy create(Collection<Host> hosts) {
                return new DCAwareRoundRobin(hosts, localDc, usedHostsPerRemoteDc);
            }
        }
    }
}
