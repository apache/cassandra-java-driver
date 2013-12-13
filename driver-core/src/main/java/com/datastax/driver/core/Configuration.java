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

import com.datastax.driver.core.policies.Policies;

/**
 * The configuration of the cluster.
 * It configures the following:
 * <ul>
 *   <li>Cassandra binary protocol level configuration (compression).</li>
 *   <li>Connection pooling configurations.</li>
 *   <li>low-level TCP configuration options (tcpNoDelay, keepAlive, ...).</li>
 * </ul>
 */
public class Configuration {

    private final Policies policies;

    private final ProtocolOptions protocolOptions;
    private final PoolingOptions poolingOptions;
    private final SocketOptions socketOptions;
    private final MetricsOptions metricsOptions;

    private final AuthInfoProvider authProvider;

    /*
     * Creates a configuration object.
     */
    public Configuration() {
        this(new Policies(),
             new ProtocolOptions(),
             new PoolingOptions(),
             new SocketOptions(),
             AuthInfoProvider.NONE,
             new MetricsOptions());
    }

    /**
     * Creates a configuration with the specified parameters.
     * 
     * @param policies the policies to use
     * @param protocolOptions the protocol options to use
     * @param poolingOptions the pooling options to use
     * @param socketOptions the socket options to use
     * @param metricsOptions the metrics options, or null to disable metrics.
     */
    public Configuration(Policies policies,
                         ProtocolOptions protocolOptions,
                         PoolingOptions poolingOptions,
                         SocketOptions socketOptions,
                         MetricsOptions metricsOptions) {
        this(policies, protocolOptions, poolingOptions, socketOptions, AuthInfoProvider.NONE, metricsOptions);
    }

    // TODO: ultimately we should expose this, but we don't want to expose the AuthInfoProvider yet as it
    // will change soon
    Configuration(Policies policies,
                         ProtocolOptions protocolOptions,
                         PoolingOptions poolingOptions,
                         SocketOptions socketOptions,
                         AuthInfoProvider authProvider,
                         MetricsOptions metricsOptions) {
        this.policies = policies;
        this.protocolOptions = protocolOptions;
        this.poolingOptions = poolingOptions;
        this.socketOptions = socketOptions;
        this.authProvider = authProvider;
        this.metricsOptions = metricsOptions;
    }

    void register(Cluster.Manager manager) {
        protocolOptions.register(manager);
        poolingOptions.register(manager);
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
     * Returns the authentication provider used to connect to the Cassandra cluster.
     *
     * @return the authentication provider in use.
     */
    // Not exposed yet on purpose
    AuthInfoProvider getAuthInfoProvider() {
        return authProvider;
    }
}
