package com.datastax.driver.core;

import java.net.InetSocketAddress;

class ConnectionException extends Exception
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
