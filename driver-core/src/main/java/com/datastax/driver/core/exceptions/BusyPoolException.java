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
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Indicates that a connection pool has run out of available connections.
 * <p/>
 * This happens if the pool has no connections (for example if it's currently reconnecting to its host), or if all
 * connections have reached their maximum number of in flight queries. The query will be retried on the next host in the
 * {@link com.datastax.driver.core.policies.LoadBalancingPolicy#newQueryPlan(String, Statement) query plan}.
 * <p/>
 * This exception is a symptom that the driver is experiencing a high workload. If it happens regularly on all hosts,
 * you should consider tuning one (or a combination of) the following pooling options:
 * <ul>
 * <li>{@link com.datastax.driver.core.PoolingOptions#setMaxRequestsPerConnection(HostDistance, int)}: maximum number of
 * requests per connection;</li>
 * <li>{@link com.datastax.driver.core.PoolingOptions#setMaxConnectionsPerHost(HostDistance, int)}: maximum number of
 * connections in the pool;</li>
 * <li>{@link com.datastax.driver.core.PoolingOptions#setMaxQueueSize(int)}: maximum number of enqueued requests before
 * this exception is thrown.</li>
 * </ul>
 */
public class BusyPoolException extends DriverException implements CoordinatorException {

    private static final long serialVersionUID = 0;

    private final InetSocketAddress address;

    public BusyPoolException(InetSocketAddress address, int queueSize) {
        this(address, buildMessage(address, queueSize), null);
    }

    public BusyPoolException(InetSocketAddress address, long timeout, TimeUnit unit) {
        this(address, buildMessage(address, timeout, unit), null);
    }

    private BusyPoolException(InetSocketAddress address, String message, Throwable cause) {
        super(message, cause);
        this.address = address;
    }

    private static String buildMessage(InetSocketAddress address, int queueSize) {
        return String.format("[%s] Pool is busy (no available connection and the queue has reached its max size %d)",
                address.getAddress(),
                queueSize);
    }

    private static String buildMessage(InetSocketAddress address, long timeout, TimeUnit unit) {
        return String.format("[%s] Pool is busy (no available connection and timed out after %d %s)",
                address.getAddress(),
                timeout, unit);
    }

    @Override
    public InetAddress getHost() {
        return address.getAddress();
    }

    @Override
    public InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public BusyPoolException copy() {
        return new BusyPoolException(address, getMessage(), this);
    }

}
