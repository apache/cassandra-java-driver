package com.datastax.driver.core.configuration;

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

    private final SocketOptions socketOptions = new SocketOptions();
    private final ProtocolOptions protocolOptions = new ProtocolOptions();
    private final PoolingOptions poolingOptions = new PoolingOptions();

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
