package com.datastax.driver.core;

import java.net.InetAddress;

/**
 * A connection exception that has to do with the transport itself, i.e. that
 * suggests the node is down.
 */
class TransportException extends ConnectionException
{
    public TransportException(InetAddress address, String msg, Throwable cause)
    {
        super(address, msg, cause);
    }

    public TransportException(InetAddress address, String msg)
    {
        super(address, msg);
    }
}
