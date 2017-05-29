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
 * Base class for tests that want to wrap a policy to add some instrumentation.
 * <p/>
 * NB: this is currently only used in tests, but could be provided as a convenience in the production code.
 */
public abstract class DelegatingLoadBalancingPolicy implements ChainableLoadBalancingPolicy {
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
        delegate.close();
    }
}