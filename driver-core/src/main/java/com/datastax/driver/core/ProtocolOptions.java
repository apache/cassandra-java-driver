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

import org.apache.cassandra.transport.FrameCompressor;

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
        SNAPPY("snappy", FrameCompressor.SnappyCompressor.instance);

        final String protocolName;
        final FrameCompressor compressor;

        private Compression(String protocolName, FrameCompressor compressor) {
            this.protocolName = protocolName;
            this.compressor = compressor;
        }

        FrameCompressor compressor() {
            return compressor;
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
    private volatile Compression compression = Compression.NONE;

    private volatile Cluster.Manager manager;

    /**
     * Creates a new {@code ProtocolOptions} instance using the {@code DEFAULT_PORT}.
     */
    public ProtocolOptions() {
        this(DEFAULT_PORT);
    }

    /**
     * Creates a new {@code ProtocolOptions} instance using the provided port.
     *
     * @param port the port to use for the binary protocol.
     */
    public ProtocolOptions(int port) {
        this.port = port;
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
}
