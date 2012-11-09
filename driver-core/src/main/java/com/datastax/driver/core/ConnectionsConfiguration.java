package com.datastax.driver.core;

/**
 * Handle all configuration related of the connections to the Cassandra hosts.
 *
 * This handle setting:
 * <ul>
 *   <li>low-level tcp configuration options (tcpNoDelay, keepAlive, ...).</li>
 *   <li>Cassandra binary protocol level configuration (compression).</li>
 *   <li>Connection pooling configurations.</li>
 * </ul>
 */
public class ConnectionsConfiguration {

    private final SocketOptions socketOptions;
    private final ProtocolOptions protocolOptions;
    private final PoolingOptions poolingOptions;

    ConnectionsConfiguration(Cluster.Manager manager) {
        this.socketOptions = new SocketOptions();
        this.protocolOptions = new ProtocolOptions(manager);
        this.poolingOptions = new PoolingOptions();
    }

    /**
     * The socket options.
     *
     * @return the socket options.
     */
    public SocketOptions getSocketOptions() {
        return socketOptions;
    }

    /**
     * The protocol options.
     *
     * @return the protocol options.
     */
    public ProtocolOptions getProtocolOptions() {
        return protocolOptions;
    }

    /**
     * The pooling options.
     *
     * @return the pooling options.
     */
    public PoolingOptions getPoolingOptions() {
        return poolingOptions;
    }
}
