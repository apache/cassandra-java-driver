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

/**
 * Options to configure low-level socket options for the connections kept
 * to the Cassandra hosts.
 */
public class SocketOptions {

    /**
     * The default connection timeout in milliseconds if none is set explicitly
     * using {@link #setConnectTimeoutMillis}.
     * <p/>
     * That default is of 5 seconds.
     */
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000;

    /**
     * The default read timeout in milliseconds if none is set explicitly
     * using {@link #setReadTimeoutMillis}.
     * <p/>
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
    private volatile Boolean tcpNoDelay = Boolean.TRUE;
    private volatile Integer receiveBufferSize;
    private volatile Integer sendBufferSize;

    /**
     * Creates a new {@code SocketOptions} instance with default values.
     */
    public SocketOptions() {
    }

    /**
     * The connection timeout in milliseconds.
     * <p/>
     * As the name implies, the connection timeout defines how long the driver
     * waits to establish a new connection to a Cassandra node before giving up.
     *
     * @return the connection timeout in milliseconds
     */
    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    /**
     * Sets the connection timeout in milliseconds.
     * <p/>
     * The default value is {@link #DEFAULT_CONNECT_TIMEOUT_MILLIS}.
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
     * <p/>
     * This defines how long the driver will wait for a given Cassandra node to
     * answer a query.
     * <p/>
     * Please note that this is not the maximum time a call to {@link Session#execute} may block;
     * this is the maximum time that a call will wait for one particular
     * Cassandra host, but other hosts could be tried if one of them times out, depending
     * on the {@link com.datastax.driver.core.policies.RetryPolicy} in use. In
     * other words, a {@link Session#execute} call may theoretically wait up to
     * {@code getReadTimeoutMillis() * <number_of_cassandra_hosts>} (though the
     * total number of hosts tried for a given query also depends on the
     * {@link com.datastax.driver.core.policies.LoadBalancingPolicy} in use).
     * If you want to control how long to wait for a query, use {@link Session#executeAsync}
     * and the {@code ResultSetFuture#get(long, TimeUnit)} method.
     * <p/>
     * Also note that for efficiency reasons, this read timeout is approximate: it
     * has an accuracy of up to 100 milliseconds (i.e. it may fire up to 100 milliseconds late).
     * It is not meant to be used for precise timeout, but rather as a protection
     * against misbehaving Cassandra nodes.
     * <p/>
     *
     * @return the read timeout in milliseconds.
     */
    public int getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    /**
     * Sets the per-host read timeout in milliseconds.
     * <p/>
     * When setting this value, keep in mind the following:
     * <ul>
     * <li>it should be higher than the timeout settings used on the Cassandra side
     * ({@code *_request_timeout_in_ms} in {@code cassandra.yaml}).</li>
     * <li>the read timeout is only approximate and only control the timeout to one Cassandra
     * host, not the full query (see {@link #getReadTimeoutMillis} for more details). If a
     * high level of precision on the timeout to a request is required, you should use
     * the {@link ResultSetFuture#get(long, java.util.concurrent.TimeUnit)} method.
     * </li>
     * </ul>
     * <p/>
     * Setting a value of 0 disables read timeouts.
     * <p/>
     * The default value is {@link #DEFAULT_READ_TIMEOUT_MILLIS}.
     *
     * @param readTimeoutMillis the timeout to set.
     * @return this {@code SocketOptions}.
     */
    public SocketOptions setReadTimeoutMillis(int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
        return this;
    }

    /**
     * Returns whether TCP keepalive is enabled.
     *
     * @return the value of the option, or {@code null} if it is not set.
     * @see #setKeepAlive(boolean)
     */
    public Boolean getKeepAlive() {
        return keepAlive;
    }

    /**
     * Sets whether to enable TCP keepalive.
     * <p/>
     * By default, this option is not set by the driver. The actual value will be the default
     * from the underlying Netty transport (Java NIO or native epoll).
     *
     * @param keepAlive whether to enable or disable the option.
     * @return this {@code SocketOptions}.
     * @see java.net.SocketOptions#TCP_NODELAY
     */
    public SocketOptions setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    /**
     * Returns whether reuse-address is enabled.
     *
     * @return the value of the option, or {@code null} if it is not set.
     * @see #setReuseAddress(boolean)
     */
    public Boolean getReuseAddress() {
        return reuseAddress;
    }

    /**
     * Sets whether to enable reuse-address.
     * <p/>
     * By default, this option is not set by the driver. The actual value will be the default
     * from the underlying Netty transport (Java NIO or native epoll).
     *
     * @param reuseAddress whether to enable or disable the option.
     * @return this {@code SocketOptions}.
     * @see java.net.SocketOptions#SO_REUSEADDR
     */
    public SocketOptions setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
        return this;
    }

    /**
     * Returns the linger-on-close timeout.
     *
     * @return the value of the option, or {@code null} if it is not set.
     * @see #setSoLinger(int)
     */
    public Integer getSoLinger() {
        return soLinger;
    }

    /**
     * Sets the linger-on-close timeout.
     * <p/>
     * By default, this option is not set by the driver. The actual value will be the default
     * from the underlying Netty transport (Java NIO or native epoll).
     *
     * @param soLinger the new value.
     * @return this {@code SocketOptions}.
     * @see java.net.SocketOptions#SO_LINGER
     */
    public SocketOptions setSoLinger(int soLinger) {
        this.soLinger = soLinger;
        return this;
    }

    /**
     * Returns whether Nagle's algorithm is disabled.
     *
     * @return the value of the option ({@code true} means Nagle is disabled), or {@code null} if it is not set.
     * @see #setTcpNoDelay(boolean)
     */
    public Boolean getTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Sets whether to disable Nagle's algorithm.
     * <p/>
     * By default, this option is set to {@code true} (Nagle disabled).
     *
     * @param tcpNoDelay whether to enable or disable the option.
     * @return this {@code SocketOptions}.
     * @see java.net.SocketOptions#TCP_NODELAY
     */
    public SocketOptions setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    /**
     * Returns the hint to the size of the underlying buffers for incoming network I/O.
     *
     * @return the value of the option, or {@code null} if it is not set.
     * @see #setReceiveBufferSize(int)
     */
    public Integer getReceiveBufferSize() {
        return receiveBufferSize;
    }

    /**
     * Sets a hint to the size of the underlying buffers for incoming network I/O.
     * <p/>
     * By default, this option is not set by the driver. The actual value will be the default
     * from the underlying Netty transport (Java NIO or native epoll).
     *
     * @param receiveBufferSize the new value.
     * @return this {@code SocketOptions}.
     * @see java.net.SocketOptions#SO_RCVBUF
     */
    public SocketOptions setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
        return this;
    }

    /**
     * Returns the hint to the size of the underlying buffers for outgoing network I/O.
     *
     * @return the value of the option, or {@code null} if it is not set.
     * @see #setSendBufferSize(int)
     */
    public Integer getSendBufferSize() {
        return sendBufferSize;
    }

    /**
     * Sets a hint to the size of the underlying buffers for outgoing network I/O.
     * <p/>
     * By default, this option is not set by the driver. The actual value will be the default
     * from the underlying Netty transport (Java NIO or native epoll).
     *
     * @param sendBufferSize the new value.
     * @return this {@code SocketOptions}.
     * @see java.net.SocketOptions#SO_SNDBUF
     */
    public SocketOptions setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        return this;
    }
}
