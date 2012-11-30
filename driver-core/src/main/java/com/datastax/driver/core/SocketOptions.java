package com.datastax.driver.core;

/**
 * Options to configure low-level socket options for the connections kept
 * to the Cassandra hosts.
 */
public class SocketOptions {

    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000;

    private volatile int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT_MILLIS;
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

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public SocketOptions setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
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
