package com.datastax.driver.core;

import com.datastax.driver.core.policies.*;

/**
 * The configuration of the cluster.
 * This handle setting:
 * <ul>
 *   <li>Cassandra binary protocol level configuration (compression).</li>
 *   <li>Connection pooling configurations.</li>
 *   <li>low-level tcp configuration options (tcpNoDelay, keepAlive, ...).</li>
 * </ul>
 */
public class Configuration {

    private final Policies policies;

    private final ProtocolOptions protocolOptions;
    private final PoolingOptions poolingOptions;
    private final SocketOptions socketOptions;

    private final AuthInfoProvider authProvider;
    private final boolean metricsEnabled;
    private final EncryptionOptions encryptionOptions;

    public Configuration() {
        this(new Policies(),
             new ProtocolOptions(),
             new PoolingOptions(),
             new SocketOptions(),
             AuthInfoProvider.NONE,
             true,
             new EncryptionOptions());
    }

    public Configuration(Policies policies,
                         ProtocolOptions protocolOptions,
                         PoolingOptions poolingOptions,
                         SocketOptions socketOptions,
                         AuthInfoProvider authProvider,
                         boolean metricsEnabled,
                         EncryptionOptions encryptionOptions) {
        this.policies = policies;
        this.protocolOptions = protocolOptions;
        this.poolingOptions = poolingOptions;
        this.socketOptions = socketOptions;
        this.authProvider = authProvider;
        this.metricsEnabled = metricsEnabled;
        this.encryptionOptions = encryptionOptions;
    }

    void register(Cluster.Manager manager) {
        protocolOptions.register(manager);
        poolingOptions.register(manager);
    }

    /**
     * The policies set for the cluster.
     *
     * @return the policies set for the cluster.
     */
    public Policies getPolicies() {
        return policies;
    }

    /**
     * The low-level tcp configuration options used (tcpNoDelay, keepAlive, ...).
     *
     * @return the socket options.
     */
    public SocketOptions getSocketOptions() {
        return socketOptions;
    }

    /**
     * The Cassandra binary protocol level configuration (compression).
     *
     * @return the protocol options.
     */
    public ProtocolOptions getProtocolOptions() {
        return protocolOptions;
    }

    /**
     * The connection pooling configuration.
     *
     * @return the pooling options.
     */
    public PoolingOptions getPoolingOptions() {
        return poolingOptions;
    }

    /**
     * The authentication provider used to connect to the Cassandra cluster.
     *
     * @return the authentication provider in use.
     */
    public AuthInfoProvider getAuthInfoProvider() {
        return authProvider;
    }

    /**
     * Whether metrics collection is enabled for the cluster instance.
     * <p>
     * Metrics collection is enabled by default but can be disabled at cluster
     * construction time through {@link Cluster.Builder#withoutMetrics}.
     *
     * @return whether metrics collection is enabled for the cluster instance.
     */
    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    /**
     * The encryption options to use when communicating with the server (SSL).
     *
     * @return SSL options.
     */
    public EncryptionOptions getEncryptionOptions()
    {
        return encryptionOptions;
    }
}
