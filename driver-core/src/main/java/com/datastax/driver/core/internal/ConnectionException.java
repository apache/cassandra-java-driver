package com.datastax.driver.core.internal;

public class ConnectionException extends Exception
{
    public final InetSocketAddress address;

    public ConnectionException(InetSocketAddress address, String msg, Throwable cause)
    {
        this(msg, cause);
        this.address = address;
    }

    public ConnectionException(InetSocketAddress address, String msg)
    {
        this(msg);
        this.address = address;
    }

    @Override
    public getMessage() {
        return String.format("[%s] %s", address, super.getMessage());
    }
}
