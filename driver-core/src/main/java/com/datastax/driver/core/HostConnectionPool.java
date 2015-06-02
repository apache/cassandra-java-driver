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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * A set of connections to a live host.
 *
 * We use different strategies depending of the protocol version in use.
 */
abstract class HostConnectionPool {

    static HostConnectionPool newInstance(Host host, HostDistance hostDistance, SessionManager manager, ProtocolVersion version) {
        switch (version) {
            case V1:
            case V2:
                return new DynamicConnectionPool(host, hostDistance, manager);
            case V3:
                return new SingleConnectionPool(host, hostDistance, manager);
            default:
                throw version.unsupported();
        }
    }

    final Host host;
    volatile HostDistance hostDistance;
    protected final SessionManager manager;

    protected final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

    protected enum Phase { INITIALIZING, READY, INIT_FAILED, CLOSING }
    protected final AtomicReference<Phase> phase = new AtomicReference<Phase>(Phase.INITIALIZING);

    protected HostConnectionPool(Host host, HostDistance hostDistance, SessionManager manager) {
        assert hostDistance != HostDistance.IGNORED;
        this.host = host;
        this.hostDistance = hostDistance;
        this.manager = manager;
    }

    /**
     * @param reusedConnection an existing connection (from a reconnection attempt) that we want to
     *                         reuse as part of this pool. Might be null or already used by another
     *                         pool.
     */
    abstract ListenableFuture<Void> initAsync(Connection reusedConnection);

    abstract Connection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException;

    abstract void returnConnection(Connection connection);

    abstract void ensureCoreConnections();

    abstract void replaceDefunctConnection(final Connection connection);

    abstract void cleanupIdleConnections(long now);

    abstract int opened();

    abstract int trashed();

    abstract int inFlightQueriesCount();

    protected abstract CloseFuture makeCloseFuture();

    public final boolean isClosed() {
        return closeFuture.get() != null;
    }

    public final CloseFuture closeAsync() {

        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        phase.set(Phase.CLOSING);

        future = makeCloseFuture();

        return closeFuture.compareAndSet(null, future)
            ? future
            : closeFuture.get(); // We raced, it's ok, return the future that was actually set
    }

    static class PoolState {
        volatile String keyspace;

        public void setKeyspace(String keyspace) {
            this.keyspace = keyspace;
        }
    }
}