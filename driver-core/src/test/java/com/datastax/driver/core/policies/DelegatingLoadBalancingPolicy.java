package com.datastax.driver.core.policies;

import java.util.Collection;
import java.util.Iterator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;

/**
 * Base class for tests that want to wrap a policy to add some instrumentation.
 *
 * NB: this is currently only used in tests, but could be provided as a convenience in the production code.
 */
public abstract class DelegatingLoadBalancingPolicy implements ChainableLoadBalancingPolicy, CloseableLoadBalancingPolicy {
    protected final LoadBalancingPolicy delegate;

    public DelegatingLoadBalancingPolicy(LoadBalancingPolicy delegate) {
        this.delegate = delegate;
    }

    public void init(Cluster cluster, Collection<Host> hosts) {
        delegate.init(cluster, hosts);
    }

    public HostDistance distance(Host host) {
        return delegate.distance(host);
    }

    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
        return delegate.newQueryPlan(loggedKeyspace, statement);
    }

    public void onAdd(Host host) {
        delegate.onAdd(host);
    }

    public void onUp(Host host) {
        delegate.onUp(host);
    }

    public void onSuspected(Host host) {
        delegate.onSuspected(host);
    }

    public void onDown(Host host) {
        delegate.onDown(host);
    }

    public void onRemove(Host host) {
        delegate.onRemove(host);
    }

    @Override
    public LoadBalancingPolicy getChildPolicy() {
        return delegate;
    }

    @Override
    public void close() {
        if (delegate instanceof CloseableLoadBalancingPolicy)
            ((CloseableLoadBalancingPolicy)delegate).close();
    }
}