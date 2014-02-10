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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Options of the Cassandra native binary protocol.
 */
public class ProtocolOptions {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolOptions.class);

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

    private final int port;

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
        this(port, null, AuthProvider.NONE);
    }

    /**
     * Creates a new {@code ProtocolOptions} instance using the provided port
     * and SSL context.
     *
     * @param port the port to use for the binary protocol.
     * @param sslOptions the SSL options to use. Use {@code null} if SSL is not
     * to be used.
     * @param authProvider the {@code AuthProvider} to use for authentication against
     * the Cassandra nodes.
     */
    public ProtocolOptions(int port, SSLOptions sslOptions, AuthProvider authProvider) {
        this.port = port;
        this.sslOptions = sslOptions;
        this.authProvider = authProvider;
    }

    void register(Cluster.Manager manager) {
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
