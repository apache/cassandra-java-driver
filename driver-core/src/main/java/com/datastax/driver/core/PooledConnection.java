/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A connection that is associated to a pool.
 */
class PooledConnection extends Connection {

    private final HostConnectionPool pool;

    /** The instant when the connection should be trashed after being idle for too long */
    private volatile long trashTime = Long.MAX_VALUE;

    /** Used in {@link DynamicConnectionPool} to handle races between two threads trying to trash the same connection */
    final AtomicBoolean markForTrash = new AtomicBoolean();

    PooledConnection(String name, InetSocketAddress address, Factory factory, HostConnectionPool pool) throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
        super(name, address, factory);
        this.pool = pool;
    }

    /**
     * Return the pooled connection to its pool.
     * The connection should generally not be reused after that.
     */
    public void release() {
        // This can happen if the query to initialize the transport in the
        // parent constructor times out. In that case the pool will handle
        // it itself.
        if (pool == null)
            return;

        pool.returnConnection(this);
    }

    @Override
    protected void notifyOwnerWhenDefunct(boolean hostIsDown) {
        // This can happen if an exception is thrown at construction time. In
        // that case the pool will handle it itself.
        if (pool == null)
            return;

        if (hostIsDown) {
            pool.closeAsync().force();
        } else {
            pool.replaceDefunctConnection(this);
        }
    }

    long getTrashTime() {
        return trashTime;
    }

    void cancelTrashTime() {
        trashTime = Long.MAX_VALUE;
    }

    void setTrashTimeIn(int timeoutSeconds) {
        trashTime = System.currentTimeMillis() + 1000 * timeoutSeconds;
    }
}
