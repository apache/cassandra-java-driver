package com.datastax.driver.core;

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The policy that decides which Cassandra hosts to contact for each new query.
 *
 * The main method to implement is {@link LoadBalancingPolicy#newQueryPlan} and
 * is used for each query to find which host to query first, and which hosts to
 * use as failover.
 *
 * The {@code LoadBalancingPolicy} is a {@link Host.StateListener} and is thus
 * informed of hosts up/down events. For efficiency purposes, the policy is
 * expected to exclude down hosts from query plans.
 */
public interface LoadBalancingPolicy extends Host.StateListener {

    /**
     * Returns the hosts to use for a given query.
     *
     * Each new query will call this method. The first host in the result will
     * then be used to perform the query. In the even of a connection problem
     * (the queried host is down or appear to be so), the next host will be
     * used. If all hosts of the returned {@code Iterator} are down, the query
     * will fail.
     *
     * @return an iterator of Host. The query is tried against the hosts
     * returned by this iterator in order, until the query has been sent
     * successfully to one of the host.
     */
    public Iterator<Host> newQueryPlan();

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
     *
     * This policy queries nodes in a round-robin fashion. For a given query,
     * if an host fail, the next one (following the round-robin order) is
     * tried, until all hosts have been tried.
     */
    public static class RoundRobin implements LoadBalancingPolicy {

        private volatile Host[] liveHosts;
        private final AtomicInteger index = new AtomicInteger();

        private RoundRobin(Collection<Host> hosts) {
            this.liveHosts = hosts.toArray(new Host[hosts.size()]);
            this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
        }

        public Iterator<Host> newQueryPlan() {

            final Host[] hosts = liveHosts;
            final int startIdx = index.getAndIncrement();

            // Overflow protection; not theoretically thread safe but should be good enough
            if (startIdx > Integer.MAX_VALUE - 10000)
                index.set(0);

            return new Iterator<Host>() {

                private int idx = startIdx;
                private int remaining = hosts.length;

                public boolean hasNext() {
                    return remaining > 0;
                }

                public Host next() {
                    Host h = hosts[idx++ % hosts.length];
                    remaining--;
                    return h;
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        public synchronized void onUp(Host host) {

            for (Host h : liveHosts)
                if (h.equals(host))
                    return;

            Host[] newHosts = new Host[liveHosts.length + 1];
            System.arraycopy(liveHosts, 0, newHosts, 0, liveHosts.length);
            newHosts[newHosts.length - 1] = host;
            liveHosts = newHosts;
        }

        public synchronized void onDown(Host host) {
            int idx = -1;
            for (int i = 0; i < liveHosts.length; i++) {
                if (liveHosts[i].equals(host)) {
                    idx = i;
                    break;
                }
            }

            if (idx == -1)
                return;

            Host[] newHosts = new Host[liveHosts.length - 1];
            if (idx == 0) {
                System.arraycopy(liveHosts, 1, newHosts, 0, newHosts.length);
            } else if (idx == liveHosts.length - 1) {
                System.arraycopy(liveHosts, 0, newHosts, 0, newHosts.length);
            } else {
                System.arraycopy(liveHosts, 0, newHosts, 0, idx);
                System.arraycopy(liveHosts, idx + 1, newHosts, idx, liveHosts.length - idx - 1);
            }
            liveHosts = newHosts;
        }

        public void onAdd(Host host) {
            onUp(host);
        }

        public void onRemove(Host host) {
            onDown(host);
        }

        public static class Factory implements LoadBalancingPolicy.Factory {

            public static final Factory INSTANCE = new Factory();

            private Factory() {}

            public LoadBalancingPolicy create(Collection<Host> hosts) {
                return new RoundRobin(hosts);
            }
        }
    }
}
