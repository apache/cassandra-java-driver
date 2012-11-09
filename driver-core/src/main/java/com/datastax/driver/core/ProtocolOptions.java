package com.datastax.driver.core;

/**
 * Options of the Cassandra native binary protocol.
 */
public class ProtocolOptions {

    private final Cluster.Manager manager;

    ProtocolOptions(Cluster.Manager manager) {
        this.manager = manager;
    }

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
     * The port to use to connect to the Cassandra hosts.
     * <p>
     * The port must be set at cluster creation time (using {@link Cluster.Builder#withPort}
     * for instance) and cannot be changed afterwards.
     *
     * @return the port to use to connect to the Cassandra hosts.
     */
    public int getPort() {
        return manager.port;
    }

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
