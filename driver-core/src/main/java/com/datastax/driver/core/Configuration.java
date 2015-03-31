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

import com.datastax.driver.core.policies.Policies;

/**
 * The configuration of the cluster.
 * It configures the following:
 * <ul>
 *   <li>Cassandra protocol level configuration (compression).</li>
 *   <li>Connection pooling configurations.</li>
 *   <li>low-level TCP configuration options (tcpNoDelay, keepAlive, ...).</li>
 *   <li>Metrics related options.</li>
 *   <li>Query related options (default consistency level, fetchSize, ...).</li>
 *   <li>Netty layer customization options.</li>
 * </ul>
 * This is also where you get the configured policies, though those cannot be changed
 * (they are set during the built of the Cluster object).
 */
public class Configuration {

    private final Policies policies;

    private final ProtocolOptions protocolOptions;
    private final PoolingOptions poolingOptions;
    private final SocketOptions socketOptions;
    private final MetricsOptions metricsOptions;
    private final QueryOptions queryOptions;
    private final NettyOptions nettyOptions;

    /*
     * Creates a configuration object.
     */
    public Configuration() {
        this(new Policies(),
             new ProtocolOptions(),
             new PoolingOptions(),
             new SocketOptions(),
             new MetricsOptions(),
             new QueryOptions(),
             NettyOptions.DEFAULT_INSTANCE);
    }

    /**
     * Creates a configuration with the specified parameters.
     * 
     * @param policies the policies to use
     * @param protocolOptions the protocol options to use
     * @param poolingOptions the pooling options to use
     * @param socketOptions the socket options to use
     * @param metricsOptions the metrics options, or null to disable metrics.
     * @param queryOptions defaults related to queries.
     * @param nettyOptions the {@link NettyOptions} instance to use
     */
    public Configuration(Policies policies,
                         ProtocolOptions protocolOptions,
                         PoolingOptions poolingOptions,
                         SocketOptions socketOptions,
                         MetricsOptions metricsOptions,
                         QueryOptions queryOptions,
                         NettyOptions nettyOptions) {
        this.policies = policies;
        this.protocolOptions = protocolOptions;
        this.poolingOptions = poolingOptions;
        this.socketOptions = socketOptions;
        this.metricsOptions = metricsOptions;
        this.queryOptions = queryOptions;
        this.nettyOptions = nettyOptions;
    }

    void register(Cluster.Manager manager) {
        protocolOptions.register(manager);
        poolingOptions.register(manager);
        queryOptions.register(manager);
    }

    /**
     * Returns the policies set for the cluster.
     *
     * @return the policies set for the cluster.
     */
    public Policies getPolicies() {
        return policies;
    }

    /**
     * Returns the low-level TCP configuration options used (tcpNoDelay, keepAlive, ...).
     *
     * @return the socket options.
     */
    public SocketOptions getSocketOptions() {
        return socketOptions;
    }

    /**
     * Returns the Cassandra binary protocol level configuration (compression).
     *
     * @return the protocol options.
     */
    public ProtocolOptions getProtocolOptions() {
        return protocolOptions;
    }

    /**
     * Returns the connection pooling configuration.
     *
     * @return the pooling options.
     */
    public PoolingOptions getPoolingOptions() {
        return poolingOptions;
    }

    /**
     * Returns the metrics configuration, if metrics are enabled.
     * <p>
     * Metrics collection is enabled by default but can be disabled at cluster
     * construction time through {@link Cluster.Builder#withoutMetrics}.
     *
     * @return the metrics options or {@code null} if metrics are not enabled.
     */
    public MetricsOptions getMetricsOptions() {
        return metricsOptions;
    }

    /**
     * Returns the queries configuration.
     *
     * @return the queries options.
     */
    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    /**
     * Returns the {@link NettyOptions} instance for this configuration.
     * @return the {@link NettyOptions} instance for this configuration.
     */
    public NettyOptions getNettyOptions() {
        return nettyOptions;
    }
}
