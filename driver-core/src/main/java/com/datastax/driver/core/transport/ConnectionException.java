package com.datastax.driver.core.transport;

import java.net.InetSocketAddress;

public class ConnectionException extends Exception
{
    public final InetSocketAddress address;

    public ConnectionException(InetSocketAddress address, String msg, Throwable cause)
    {
        super(msg, cause);
        this.address = address;
    }

    public ConnectionException(InetSocketAddress address, String msg)
    {
        super(msg);
        this.address = address;
    }

    @Override
    public String getMessage() {
        return String.format("[%s] %s", address, super.getMessage());
    }
}
