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

    /**
     * Options to configure low-level socket options for the connections kept
     * to the Cassandra hosts.
     */
    public static class SocketOptions {

        public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000;

        private volatile int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT_MILLIS;
        private volatile Boolean keepAlive;
        private volatile Boolean reuseAddress;
        private volatile Integer soLinger;
        private volatile Boolean tcpNoDelay;
        private volatile Integer receiveBufferSize;
        private volatile Integer sendBufferSize;

        public int getConnectTimeoutMillis() {
            return connectTimeoutMillis;
        }

        public void setConnectTimeoutMillis(int connectTimeoutMillis) {
            this.connectTimeoutMillis = connectTimeoutMillis;
        }

        public Boolean getKeepAlive() {
            return keepAlive;
        }

        public void setKeepAlive(boolean keepAlive) {
            this.keepAlive = keepAlive;
        }

        public Boolean getReuseAddress() {
            return reuseAddress;
        }

        public void setReuseAddress(boolean reuseAddress) {
            this.reuseAddress = reuseAddress;
        }

        public Integer getSoLinger() {
            return soLinger;
        }

        public void setSoLinger(int soLinger) {
            this.soLinger = soLinger;
        }

        public Boolean getTcpNoDelay() {
            return tcpNoDelay;
        }

        public void setTcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;
        }

        public Integer getReceiveBufferSize() {
            return receiveBufferSize;
        }

        public void setReceiveBufferSize(int receiveBufferSize) {
            this.receiveBufferSize = receiveBufferSize;
        }

        public Integer getSendBufferSize() {
            return sendBufferSize;
        }

        public void setSendBufferSize(int sendBufferSize) {
            this.sendBufferSize = sendBufferSize;
        }
    }

    /**
     * Options of the Cassandra native binary protocol.
     */
    public static class ProtocolOptions {

        /**
         * Compression supported by the Cassandra binary protocol.
         */
        public enum Compression {
            /** No compression */
            NONE(""),
            /** Snappy compression */
            SNAPPY("snappy");

            final String protocolName;

            private Compression(String protocolName) {
                this.protocolName = protocolName;
            }

            @Override
            public String toString() {
                return protocolName;
            }
        };

        private volatile Compression compression = Compression.NONE;

        /**
         * Returns the compression used by the protocol.
         * <p>
         * The default compression is {@code Compression.SNAPPY}.
         *
         * @return the compression used.
         */
        public Compression getCompression() {
            return compression;
        }

        /**
         * Sets the compression to use.
         * <p>
         * Note that while this setting can be changed at any time, it will
         * only apply to newly created connections.
         *
         * @param compression the compression algorithm to use (or {@code
         * Compression.NONE} to disable compression).
         * @return this {@code ProtocolOptions} object.
         */
        public ProtocolOptions setCompression(Compression compression) {
            this.compression = compression;
            return this;
        }
    }

    /**
     * Options related to connection pooling.
     * <p>
     * The driver uses connections in an asynchronous way. Meaning that
     * multiple requests can be submitted on the same connection at the same
     * time. This means that the driver only needs to maintain a relatively
     * small number of connections to each Cassandra host. These options allow
     * to control how many connections are kept exactly.
     * <p>
     * For each host, the driver keeps a core amount of connections open at all
     * time ({@link PoolingOptions#getCoreConnectionsPerHost}). If the
     * utilisation of those connections reaches a configurable threshold
     * ({@link PoolingOptions#getMaxSimultaneousRequestsPerConnectionTreshold}),
     * more connections are created up to a configurable maximum number of
     * connections ({@link PoolingOptions#getMaxConnectionPerHost}). Once more
     * than core connections have been created, connections in excess are
     * reclaimed if the utilisation of opened connections drops below the
     * configured threshold ({@link PoolingOptions#getMinSimultaneousRequestsPerConnectionTreshold}).
     * <p>
     * Each of these parameters can be separately set for {@code LOCAL} and
     * {@code REMOTE} hosts ({@link HostDistance}). For {@code IGNORED} hosts,
     * the default for all those settings is 0 and cannot be changed.
     */
    public static class PoolingOptions {

        // Note: we could use an enumMap or similar, but synchronization would
        // be more costly so let's stick to volatile in for now.
        private static final int DEFAULT_MIN_REQUESTS = 25;
        private static final int DEFAULT_MAX_REQUESTS = 100;

        private static final int DEFAULT_CORE_POOL_LOCAL = 2;
        private static final int DEFAULT_CORE_POOL_REMOTE = 1;

        private static final int DEFAULT_MAX_POOL_LOCAL = 8;
        private static final int DEFAULT_MAX_POOL_REMOTE = 2;

        private volatile int minSimultaneousRequestsForLocal = DEFAULT_MIN_REQUESTS;
        private volatile int minSimultaneousRequestsForRemote = DEFAULT_MIN_REQUESTS;

        private volatile int maxSimultaneousRequestsForLocal = DEFAULT_MAX_REQUESTS;
        private volatile int maxSimultaneousRequestsForRemote = DEFAULT_MAX_REQUESTS;

        private volatile int coreConnectionsForLocal = DEFAULT_CORE_POOL_LOCAL;
        private volatile int coreConnectionsForRemote = DEFAULT_CORE_POOL_REMOTE;

        private volatile int maxConnectionsForLocal = DEFAULT_MAX_POOL_LOCAL;
        private volatile int maxConnectionsForRemote = DEFAULT_MAX_POOL_REMOTE;

        /**
         * Number of simultaneous requests on a connection below which
         * connections in excess are reclaimed.
         * <p>
         * If an opened connection to an host at distance {@code distance}
         * handles less than this number of simultaneous requests and there is
         * more than {@link #getCoreConnectionsPerHost} connections open to this
         * host, the connection is closed.
         * <p>
         * The default value for this option is 25 for {@code LOCAL} and
         * {@code REMOTE} hosts.
         *
         * @param distance the {@code HostDistance} for which to return this threshold.
         * @return the configured threshold, or the default one if none have been set.
         */
        public int getMinSimultaneousRequestsPerConnectionTreshold(HostDistance distance) {
            switch (distance) {
                case LOCAL:
                    return minSimultaneousRequestsForLocal;
                case REMOTE:
                    return minSimultaneousRequestsForRemote;
                default:
                    return 0;
            }
        }

        /**
         * Sets the number of simultaneous requests on a connection below which
         * connections in excess are reclaimed.
         *
         * @param distance the {@code HostDistance} for which to configure this threshold.
         * @param minSimultaneousRequests the value to set.
         * @return this {@code PoolingOptions}.
         *
         * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}.
         */
        public PoolingOptions setMinSimultaneousRequestsPerConnectionTreshold(HostDistance distance, int minSimultaneousRequests) {
            switch (distance) {
                case LOCAL:
                    minSimultaneousRequestsForLocal = minSimultaneousRequests;
                    break;
                case REMOTE:
                    minSimultaneousRequestsForRemote = minSimultaneousRequests;
                    break;
                default:
                    throw new IllegalArgumentException("Cannot set min streams per connection threshold for " + distance + " hosts");
            }
            return this;
        }

        /**
         * Number of simultaneous requests on all connections to an host after
         * which more connections are created.
         * <p>
         * If all the connections opened to an host at distance {@code
         * distance} connection are handling more than this number of
         * simultaneous requests and there is less than
         * {@link #getMaxConnectionPerHost} connections open to this host, a
         * new connection is open.
         * <p>
         * Note that a given connection cannot handle more than 128
         * simultaneous requests (protocol limitation).
         * <p>
         * The default value for this option is 100 for {@code LOCAL} and
         * {@code REMOTE} hosts.
         *
         * @param distance the {@code HostDistance} for which to return this threshold.
         * @return the configured threshold, or the default one if none have been set.
         */
        public int getMaxSimultaneousRequestsPerConnectionTreshold(HostDistance distance) {
            switch (distance) {
                case LOCAL:
                    return maxSimultaneousRequestsForLocal;
                case REMOTE:
                    return maxSimultaneousRequestsForRemote;
                default:
                    return 0;
            }
        }

        /**
         * Sets number of simultaneous requests on all connections to an host after
         * which more connections are created.
         *
         * @param distance the {@code HostDistance} for which to configure this threshold.
         * @param maxSimultaneousRequests the value to set.
         * @return this {@code PoolingOptions}.
         *
         * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}.
         */
        public PoolingOptions setMaxSimultaneousRequestsPerConnectionTreshold(HostDistance distance, int maxSimultaneousRequests) {
            switch (distance) {
                case LOCAL:
                    maxSimultaneousRequestsForLocal = maxSimultaneousRequests;
                    break;
                case REMOTE:
                    maxSimultaneousRequestsForRemote = maxSimultaneousRequests;
                    break;
                default:
                    throw new IllegalArgumentException("Cannot set max streams per connection threshold for " + distance + " hosts");
            }
            return this;
        }

        /**
         * The core number of connections per host.
         * <p>
         * For the provided {@code distance}, this correspond to the number of
         * connections initially created and kept open to each host of that
         * distance.
         *
         * @param distance the {@code HostDistance} for which to return this threshold.
         * @return the core number of connections per host at distance {@code distance}.
         */
        public int getCoreConnectionsPerHost(HostDistance distance) {
            switch (distance) {
                case LOCAL:
                    return coreConnectionsForLocal;
                case REMOTE:
                    return coreConnectionsForRemote;
                default:
                    return 0;
            }
        }

        /**
         * Sets the core number of connections per host.
         *
         * @param distance the {@code HostDistance} for which to set this threshold.
         * @param coreConnections the value to set
         * @return this {@code PoolingOptions}.
         *
         * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}.
         */
        public PoolingOptions setCoreConnectionsPerHost(HostDistance distance, int coreConnections) {
            // TODO: make sure the pools are updated accordingly
            switch (distance) {
                case LOCAL:
                    coreConnectionsForLocal = coreConnections;
                    break;
                case REMOTE:
                    coreConnectionsForRemote = coreConnections;
                    break;
                default:
                    throw new IllegalArgumentException("Cannot set core connections per host for " + distance + " hosts");
            }
            return this;
        }

        /**
         * The maximum number of connections per host.
         * <p>
         * For the provided {@code distance}, this correspond to the maximum
         * number of connections that can be created per host at that distance.
         *
         * @param distance the {@code HostDistance} for which to return this threshold.
         * @return the maximum number of connections per host at distance {@code distance}.
         */
        public int getMaxConnectionPerHost(HostDistance distance) {
            switch (distance) {
                case LOCAL:
                    return maxConnectionsForLocal;
                case REMOTE:
                    return maxConnectionsForRemote;
                default:
                    return 0;
            }
        }

        /**
         * Sets the maximum number of connections per host.
         *
         * @param distance the {@code HostDistance} for which to set this threshold.
         * @param maxConnections the value to set
         * @return this {@code PoolingOptions}.
         *
         * @throws IllegalArgumentException if {@code distance == HostDistance.IGNORED}.
         */
        public PoolingOptions setMaxConnectionsPerHost(HostDistance distance, int maxConnections) {
            // TODO: make sure the pools are updated accordingly
            switch (distance) {
                case LOCAL:
                    maxConnectionsForLocal = maxConnections;
                    break;
                case REMOTE:
                    maxConnectionsForRemote = maxConnections;
                    break;
                default:
                    throw new IllegalArgumentException("Cannot set max connections per host for " + distance + " hosts");
            }
            return this;
        }
    }
}
