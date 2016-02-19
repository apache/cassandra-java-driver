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

import com.datastax.driver.core.policies.Policies;

/**
 * The configuration of the cluster.
 * It configures the following:
 * <ul>
 * <li>Cassandra protocol level configuration (compression).</li>
 * <li>Connection pooling configurations.</li>
 * <li>low-level TCP configuration options (tcpNoDelay, keepAlive, ...).</li>
 * <li>Metrics related options.</li>
 * <li>Query related options (default consistency level, fetchSize, ...).</li>
 * <li>Netty layer customization options.</li>
 * </ul>
 * This is also where you get the configured policies, though those cannot be changed
 * (they are set during the built of the Cluster object).
 */
public class Configuration {

    /**
     * Returns a builder to create a new {@code Configuration} object.
     * <p/>
     * You only need this if you are building the configuration yourself. If you
     * use {@link Cluster#builder()}, it will be done under the hood for you.
     *
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    private final Policies policies;
    private final ProtocolOptions protocolOptions;
    private final PoolingOptions poolingOptions;
    private final SocketOptions socketOptions;
    private final MetricsOptions metricsOptions;
    private final QueryOptions queryOptions;
    private final NettyOptions nettyOptions;
    private final CodecRegistry codecRegistry;

    private Configuration(Policies policies,
                          ProtocolOptions protocolOptions,
                          PoolingOptions poolingOptions,
                          SocketOptions socketOptions,
                          MetricsOptions metricsOptions,
                          QueryOptions queryOptions,
                          NettyOptions nettyOptions,
                          CodecRegistry codecRegistry) {
        this.policies = policies;
        this.protocolOptions = protocolOptions;
        this.poolingOptions = poolingOptions;
        this.socketOptions = socketOptions;
        this.metricsOptions = metricsOptions;
        this.queryOptions = queryOptions;
        this.nettyOptions = nettyOptions;
        this.codecRegistry = codecRegistry;
    }

    /**
     * Copy constructor.
     *
     * @param toCopy the object to copy from.
     */
    protected Configuration(Configuration toCopy) {
        this(
                toCopy.getPolicies(),
                toCopy.getProtocolOptions(),
                toCopy.getPoolingOptions(),
                toCopy.getSocketOptions(),
                toCopy.getMetricsOptions(),
                toCopy.getQueryOptions(),
                toCopy.getNettyOptions(),
                toCopy.getCodecRegistry()
        );
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
     * <p/>
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
     *
     * @return the {@link NettyOptions} instance for this configuration.
     */
    public NettyOptions getNettyOptions() {
        return nettyOptions;
    }

    /**
     * Returns the {@link CodecRegistry} instance for this configuration.
     * <p/>
     * Note that this method could return {@link CodecRegistry#DEFAULT_INSTANCE}
     * if no specific codec registry has been set on the {@link Cluster}.
     * In this case, care should be taken when registering new codecs as they would be
     * immediately available to other {@link Cluster} instances sharing the same default instance.
     *
     * @return the {@link CodecRegistry} instance for this configuration.
     */
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    /**
     * A builder to create a new {@code Configuration} object.
     */
    public static class Builder {
        private Policies policies;
        private ProtocolOptions protocolOptions;
        private PoolingOptions poolingOptions;
        private SocketOptions socketOptions;
        private MetricsOptions metricsOptions;
        private QueryOptions queryOptions;
        private NettyOptions nettyOptions;
        private CodecRegistry codecRegistry;

        /**
         * Sets the policies for this cluster.
         *
         * @param policies the policies.
         * @return this builder.
         */
        public Builder withPolicies(Policies policies) {
            this.policies = policies;
            return this;
        }

        /**
         * Sets the protocol options for this cluster.
         *
         * @param protocolOptions the protocol options.
         * @return this builder.
         */
        public Builder withProtocolOptions(ProtocolOptions protocolOptions) {
            this.protocolOptions = protocolOptions;
            return this;
        }

        /**
         * Sets the pooling options for this cluster.
         *
         * @param poolingOptions the pooling options.
         * @return this builder.
         */
        public Builder withPoolingOptions(PoolingOptions poolingOptions) {
            this.poolingOptions = poolingOptions;
            return this;
        }

        /**
         * Sets the socket options for this cluster.
         *
         * @param socketOptions the socket options.
         * @return this builder.
         */
        public Builder withSocketOptions(SocketOptions socketOptions) {
            this.socketOptions = socketOptions;
            return this;
        }

        /**
         * Sets the metrics options for this cluster.
         * <p/>
         * If this method doesn't get called, the configuration will use the
         * defaults: metrics enabled with JMX reporting enabled.
         * To disable metrics, call this method with an instance where
         * {@link MetricsOptions#isEnabled() isEnabled()} returns false.
         *
         * @param metricsOptions the metrics options.
         * @return this builder.
         */
        public Builder withMetricsOptions(MetricsOptions metricsOptions) {
            this.metricsOptions = metricsOptions;
            return this;
        }

        /**
         * Sets the query options for this cluster.
         *
         * @param queryOptions the query options.
         * @return this builder.
         */
        public Builder withQueryOptions(QueryOptions queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }

        /**
         * Sets the Netty options for this cluster.
         *
         * @param nettyOptions the Netty options.
         * @return this builder.
         */
        public Builder withNettyOptions(NettyOptions nettyOptions) {
            this.nettyOptions = nettyOptions;
            return this;
        }

        /**
         * Sets the codec registry for this cluster.
         *
         * @param codecRegistry the codec registry.
         * @return this builder.
         */
        public Builder withCodecRegistry(CodecRegistry codecRegistry) {
            this.codecRegistry = codecRegistry;
            return this;
        }

        /**
         * Builds the final object from this builder.
         * <p/>
         * Any field that hasn't been set explicitly will get its default value.
         *
         * @return the object.
         */
        public Configuration build() {
            return new Configuration(
                    policies != null ? policies : Policies.builder().build(),
                    protocolOptions != null ? protocolOptions : new ProtocolOptions(),
                    poolingOptions != null ? poolingOptions : new PoolingOptions(),
                    socketOptions != null ? socketOptions : new SocketOptions(),
                    metricsOptions != null ? metricsOptions : new MetricsOptions(),
                    queryOptions != null ? queryOptions : new QueryOptions(),
                    nettyOptions != null ? nettyOptions : NettyOptions.DEFAULT_INSTANCE,
                    codecRegistry != null ? codecRegistry : CodecRegistry.DEFAULT_INSTANCE);
        }
    }
}
