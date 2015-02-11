package com.datastax.driver.core;

import java.net.InetSocketAddress;

/**
 * Thrown on a client-side timeout, i.e. when the client didn't hear back from the server within
 * {@link SocketOptions#getReadTimeoutMillis()}.
 */
class OperationTimedOutException extends ConnectionException {
    public OperationTimedOutException(InetSocketAddress address) {
        super(address, "Operation timed out");
    }
}
