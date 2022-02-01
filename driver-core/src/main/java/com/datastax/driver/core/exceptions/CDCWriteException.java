package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.EndPoint;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/** An attempt was made to write to a commitlog segment which doesn't support CDC mutations **/
public class CDCWriteException extends QueryExecutionException implements CoordinatorException {

    private static final long serialVersionUID = 0;

    private final EndPoint endPoint;

    public CDCWriteException(EndPoint endPoint, String message) {
        super(message);
        this.endPoint = endPoint;
    }

    /** Private constructor used solely when copying exceptions. */
    private CDCWriteException(EndPoint endPoint, String message, CDCWriteException cause) {
        super(message, cause);
        this.endPoint = endPoint;
    }

    @Override
    public EndPoint getEndPoint() {
        return endPoint;
    }

    @Override
    @Deprecated
    public InetSocketAddress getAddress() {
        return (endPoint == null) ? null : endPoint.resolve();
    }

    @Override
    @Deprecated
    public InetAddress getHost() {
        return (endPoint == null) ? null : endPoint.resolve().getAddress();
    }

    @Override
    public CDCWriteException copy() {
        return new CDCWriteException(endPoint, getMessage(), this);
    }
}
