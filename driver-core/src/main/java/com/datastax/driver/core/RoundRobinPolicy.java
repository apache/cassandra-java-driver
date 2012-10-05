package com.datastax.driver.core;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.LoadBalancingPolicy;

public class RoundRobinPolicy implements LoadBalancingPolicy {

    private volatile Host[] liveHosts;
    private final AtomicInteger index = new AtomicInteger();

    private RoundRobinPolicy(Collection<Host> hosts) {
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
            return new RoundRobinPolicy(hosts);
        }
    }
}
