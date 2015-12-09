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
package com.datastax.driver.core;

import com.google.common.util.concurrent.ListenableFuture;

import java.net.InetSocketAddress;
import java.util.Collections;

/**
 * Base class for custom {@link Cluster} implementations that wrap another instance (delegate / decorator pattern).
 */
public abstract class DelegatingCluster extends Cluster {
    /**
     * Builds a new instance.
     */
    protected DelegatingCluster() {
        // Implementation notes:
        // If Cluster was an interface, delegates would be trivial to write. But, for historical reasons, it's a class,
        // and changing that would break backward compatibility. That makes delegates rather convoluted and error-prone
        // to write, so we provide DelegatingCluster to abstract the details.
        // This class ensures that:
        // - init() is never called on the parent class, because that would initialize the Cluster.Manager instance and
        //   create a lot of internal state (thread pools, etc.) that we don't need, since another Cluster instance is
        //   already handling the calls.
        // - all public methods are properly forwarded to the delegate (otherwise they would call the parent class and
        //   return inconsistent results).
        // These two goals are closely related, since a lot of public methods call init(), so accidentally calling a
        // parent method could initialize the parent state.

        // Construct parent class with dummy parameters that will never get used (since super.init() is never called).
        super("delegating_cluster", Collections.<InetSocketAddress>emptyList(), null);

        // Immediately close the parent class's internal Manager, to make sure that it will fail fast if it's ever
        // accidentally invoked.
        super.closeAsync();
    }

    /**
     * Returns the delegate instance where all calls will be forwarded.
     *
     * @return the delegate.
     */
    protected abstract Cluster delegate();

    @Override
    public Cluster init() {
        return delegate().init();
    }

    @Override
    public Session newSession() {
        return delegate().newSession();
    }

    @Override
    public Session connect() {
        return delegate().connect();
    }

    @Override
    public Session connect(String keyspace) {
        return delegate().connect(keyspace);
    }

    @Override
    public ListenableFuture<Session> connectAsync() {
        return delegate().connectAsync();
    }

    @Override
    public ListenableFuture<Session> connectAsync(String keyspace) {
        return delegate().connectAsync(keyspace);
    }

    @Override
    public Metadata getMetadata() {
        return delegate().getMetadata();
    }

    @Override
    public Configuration getConfiguration() {
        return delegate().getConfiguration();
    }

    @Override
    public Metrics getMetrics() {
        return delegate().getMetrics();
    }

    @Override
    public Cluster register(Host.StateListener listener) {
        return delegate().register(listener);
    }

    @Override
    public Cluster unregister(Host.StateListener listener) {
        return delegate().unregister(listener);
    }

    @Override
    public Cluster register(LatencyTracker tracker) {
        return delegate().register(tracker);
    }

    @Override
    public Cluster unregister(LatencyTracker tracker) {
        return delegate().unregister(tracker);
    }

    @Override
    public CloseFuture closeAsync() {
        return delegate().closeAsync();
    }

    @Override
    public void close() {
        delegate().close();
    }

    @Override
    public boolean isClosed() {
        return delegate().isClosed();
    }
}
