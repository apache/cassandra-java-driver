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
package com.datastax.driver.core;

import java.net.InetAddress;

/**
 * A connection that is associated to a pool.
 */
class PooledConnection extends Connection {

    private final HostConnectionPool pool;

    PooledConnection(String name, InetAddress address, Factory factory, HostConnectionPool pool) throws ConnectionException, InterruptedException {
        super(name, address, factory);
        this.pool = pool;
    }

    /**
     * Return the pooled connection to it's pool.
     * The connection should generally not be reuse after that.
     */
    public void release()
    {
        pool.returnConnection(this);
    }
}
