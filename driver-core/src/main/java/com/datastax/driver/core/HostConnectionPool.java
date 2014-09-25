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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

interface HostConnectionPool {

    // This creates connections if we have less than core connections (if we
    // have more than core, connection will just get trash when we can).
    void ensureCoreConnections();

    PooledConnection borrowConnection(long timeout, TimeUnit unit) throws ConnectionException, TimeoutException;

    void returnConnection(PooledConnection connection);

    void replaceDefunctConnection(final PooledConnection connection);

    CloseFuture closeAsync();

    boolean isClosed();

    int opened();

    Host host();

    HostDistance hostDistance();

    void setHostDistance(HostDistance dist);

    int connectionsCount();

    int inFlightQueriesCount();

    static class PoolState {
        volatile String keyspace;

        public void setKeyspace(String keyspace) {
            this.keyspace = keyspace;
        }
    }
}