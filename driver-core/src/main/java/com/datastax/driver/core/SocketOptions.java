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

/**
 * Options to configure low-level socket options for the connections kept
 * to the Cassandra hosts.
 */
public class SocketOptions {

    /**
     * The default connection timeout in milliseconds if none is set explicitly
     * using {@link #setConnectTimeoutMillis}.
     * <p>
     * That default is of 5 seconds.
     */
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000;

    /**
     * The default read timeout in milliseconds if none is set explicitly
     * using {@link #setReadTimeoutMillis}.
     * <p>
     * That default is of 12 seconds so as to be slightly bigger that the
     * default Cassandra timeout.
     *
     * @see #getReadTimeoutMillis for more details on this timeout.
     */
    public static final int DEFAULT_READ_TIMEOUT_MILLIS = 12000;

    private volatile int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT_MILLIS;
    private volatile int readTimeoutMillis = DEFAULT_READ_TIMEOUT_MILLIS;
    private volatile Boolean keepAlive;
    private volatile Boolean reuseAddress;
    private volatile Integer soLinger;
    private volatile Boolean tcpNoDelay;
    private volatile Integer receiveBufferSize;
    private volatile Integer sendBufferSize;

    /**
     * Creates a new {@code SocketOptions} instance with default values.
     */
    public SocketOptions() {}

    /**
     * The connection timeout in milliseconds.
     * <p>
     * As the name implies, the connection timeout defines how long the driver
     * waits to etablish a new connection to a Cassandra node before giving up.
     *
     * @return the connection timeout in milliseconds
     */
    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    /**
     * Sets the connection timeout in milliseconds.
     *
     * @param connectTimeoutMillis the timeout to set.
     * @return this {@code SocketOptions}.
     */
    public SocketOptions setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    /**
     * The per-host read timeout in milliseconds.
     * <p>
     * This defines how long the driver will wait for a given Cassandra node to
     * answer a query.
     * <p>
     * Please note that this is not the maximum time a call to {@link Session#execute} may block;
     * this is the maximum time that call will wait for one particular
     * Cassandra host, but other hosts will be tried if one of them timeout. In
     * other words, a {@link Session#execute} call may theoretically wait up to
     * {@code getReadTimeoutMillis() * <number_of_cassandra_hosts>} (though the
     * total number of hosts tried for a given query also depends on the
     * {@link com.datastax.driver.core.policies.LoadBalancingPolicy} in use).
     * If you want to control how long to wait for a query, use {@link Session#executeAsync}
     * and the {@code ResultSetFuture#get(long, TimeUnit)} method.
     * <p>
     * Also note that for efficiency reasons, this read timeout is approximate: it
     * has an accuracy of up to 100 milliseconds (i.e. it may fire up to 100 milliseconds late).
     * It is not meant to be used for precise timeout, but rather as a protection
     * against misbehaving Cassandra nodes.
     * <p>
     *
     * @return the read timeout in milliseconds.
     */
    public int getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    /**
     * Sets the per-host read timeout in milliseconds.
     * <p>
     * When setting this value, keep in mind the following:
     * <ul>
     *   <li>the timeout settings used on the Cassandra side ({@code *_request_timeout_in_ms}
     *   in {@code cassandra.yaml}) should be taken into account when picking a value for this
     *   read timeout. In particular, if this read timeout option is lower than Cassandra's
     *   timeout, the driver will try another host before it has had the time to receive the
     *   Cassandra timeout. This might be counter-productive.</li>
     *   <li>the read timeout is only approximate and only control the timeout to one Cassandra
     *   host, not the full query (see {@link #getReadTimeoutMillis} for more details). If a
     *   high level of precision on the timeout to a request is required, you should use
     *   the {@link ResultSetFuture#get(long, TimeUnit)} method.
     *   </li>
     * </ul>
     * <p>
     * Setting a value of 0 disables read timeouts.
     *
     * @param readTimeoutMillis the timeout to set.
     * @return this {@code SocketOptions}.
     */
    public SocketOptions setReadTimeoutMillis(int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
        return this;
    }

    public Boolean getKeepAlive() {
        return keepAlive;
    }

    public SocketOptions setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public Boolean getReuseAddress() {
        return reuseAddress;
    }

    public SocketOptions setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
        return this;
    }

    public Integer getSoLinger() {
        return soLinger;
    }

    public SocketOptions setSoLinger(int soLinger) {
        this.soLinger = soLinger;
        return this;
    }

    public Boolean getTcpNoDelay() {
        return tcpNoDelay;
    }

    public SocketOptions setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public Integer getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public SocketOptions setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
        return this;
    }

    public Integer getSendBufferSize() {
        return sendBufferSize;
    }

    public SocketOptions setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        return this;
    }
}
