package com.datastax.driver.core.transport;

import java.net.InetSocketAddress;

/**
 * A connection exception that has to do with the transport itself, i.e. that
 * suggests the node is down.
 */
public class TransportException extends ConnectionException
{
    public TransportException(InetSocketAddress address, String msg, Throwable cause)
    {
        super(address, msg, cause);
    }

    public TransportException(InetSocketAddress address, String msg)
    {
        super(address, msg);
    }
}
