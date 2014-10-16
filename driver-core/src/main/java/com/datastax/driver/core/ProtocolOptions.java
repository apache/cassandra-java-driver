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

/**
 * Options of the Cassandra native binary protocol.
 */
public class ProtocolOptions {

    /**
     * Compression supported by the Cassandra binary protocol.
     */
    public enum Compression {
        /** No compression */
        NONE("", null),
        /** Snappy compression */
        SNAPPY("snappy", FrameCompressor.SnappyCompressor.instance),
        /** LZ4 compression */
        LZ4("lz4", FrameCompressor.LZ4Compressor.instance);

        final String protocolName;
        final FrameCompressor compressor;

        private Compression(String protocolName, FrameCompressor compressor) {
            this.protocolName = protocolName;
            this.compressor = compressor;
        }

        FrameCompressor compressor() {
            return compressor;
        }

        static Compression fromString(String str) {
            for (Compression c : values()) {
                if (c.protocolName.equalsIgnoreCase(str))
                    return c;
            }
            return null;
        }

        @Override
        public String toString() {
            return protocolName;
        }
    };

    /**
     * The default port for Cassandra native binary protocol: 9042.
     */
    public static final int DEFAULT_PORT = 9042;

    /**
     * The default value for {@link #getMaxSchemaAgreementWaitSeconds()}: 10.
     */
    public static final int DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS = 10;

    private volatile Cluster.Manager manager;

    private final int port;
    final int initialProtocolVersion; // What the user asked us. Will be -1 by default.

    private final int maxSchemaAgreementWaitSeconds;

    private final SSLOptions sslOptions; // null if no SSL
    private final AuthProvider authProvider;

    private volatile Compression compression = Compression.NONE;

    /**
     * Creates a new {@code ProtocolOptions} instance using the {@code DEFAULT_PORT}
     * (and without SSL).
     */
    public ProtocolOptions() {
        this(DEFAULT_PORT);
    }

    /**
     * Creates a new {@code ProtocolOptions} instance using the provided port
     * (without SSL nor authentication).
     * <p>
     * This is a shortcut for {@code new ProtocolOptions(port, null, AuthProvider.NONE)}.
     *
     * @param port the port to use for the binary protocol.
     */
    public ProtocolOptions(int port) {
        this(port, -1, DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS, null, AuthProvider.NONE);
    }

    /**
     * Creates a new {@code ProtocolOptions} instance using the provided port
     * and SSL context.
     *
     * @param port the port to use for the binary protocol.
     * @param protocolVersion the protocol version to use. This can be a negative number, in which case the
     * version uses will be the biggest version supported by the <em>first</em> node the driver connects to.
     * Otherwise, it must be either 1 or 2 to force using a particular protocol version. See
     * {@link Cluster.Builder#withProtocolVersion} for more details.
     * @param sslOptions the SSL options to use. Use {@code null} if SSL is not
     * to be used.
     * @param authProvider the {@code AuthProvider} to use for authentication against
     * the Cassandra nodes.
     */
    public ProtocolOptions(int port, int protocolVersion, int maxSchemaAgreementWaitSeconds, SSLOptions sslOptions, AuthProvider authProvider) {
        this.port = port;
        this.initialProtocolVersion = protocolVersion;
        this.maxSchemaAgreementWaitSeconds = maxSchemaAgreementWaitSeconds;
        this.sslOptions = sslOptions;
        this.authProvider = authProvider;

        if (protocolVersion >= 0 && protocolVersion != 1 && protocolVersion != 2)
            throw new IllegalArgumentException(String.format("Unsupported protocol version %d; valid values are 1, 2 or negative (for auto-detect).", protocolVersion));
    }

    void register(Cluster.Manager manager) {
        this.manager = manager;
    }

    /**
     * Returns the port used to connect to the Cassandra hosts.
     *
     * @return the port used to connect to the Cassandra hosts.
     */
    public int getPort() {
        return port;
    }

    /**
     * The protocol version used by the Cluster instance.
     *
     * @return the protocol version in use. This might return a negative value if a particular
     * version hasn't been forced by the user (using say {Cluster.Builder#withProtocolVersion})
     * <em>and</em> this Cluster instance has not yet connected to any node (but as soon as the
     * Cluster instance is connected, this is guaranteed to return either 1 or 2). Note that
     * nodes that do not support this protocol version will be ignored.
     */
    public int getProtocolVersion() {
        return manager.connectionFactory.protocolVersion;
    }

    /**
     * Returns the compression used by the protocol.
     * <p>
     * By default, compression is not used.
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
     *
     * @throws IllegalStateException if the compression requested is not
     * available. Most compression algorithms require that the relevant be
     * present in the classpath. If not, the compression will be
     * unavailable.
     */
    public ProtocolOptions setCompression(Compression compression) {
        if (compression != Compression.NONE && compression.compressor == null)
            throw new IllegalStateException("The requested compression is not available (some compression require a JAR to be found in the classpath)");

        this.compression = compression;
        return this;
    }

    /**
     * Returns the maximum time to wait for schema agreement before returning from a DDL query.
     *
     * @return the time.
     */
    public int getMaxSchemaAgreementWaitSeconds() {
        return maxSchemaAgreementWaitSeconds;
    }

    /**
     * The {@code SSLOptions} used by this cluster.
     *
     * @return the {@code SSLOptions} used by this cluster (set at the cluster creation time)
     * or {@code null} if SSL is not in use.
     */
    public SSLOptions getSSLOptions() {
        return sslOptions;
    }

    /**
     * The {@code AuthProvider} used by this cluster.
     *
     * @return the {@code AuthProvided} used by this cluster (set at the cluster creation
     * time). If no authentication mechanism is in use (the default), {@code AuthProvided.NONE}
     * will be returned.
     */
    public AuthProvider getAuthProvider() {
        return authProvider;
    }

}
