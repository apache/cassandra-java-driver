package com.datastax.driver.core;

import java.net.InetAddress;

class ConnectionException extends Exception
{
    public final InetAddress address;

    public ConnectionException(InetAddress address, String msg, Throwable cause)
    {
        super(msg, cause);
        this.address = address;
    }

    public ConnectionException(InetAddress address, String msg)
    {
        super(msg);
        this.address = address;
    }

    @Override
    public String getMessage() {
        return String.format("[%s] %s", address, super.getMessage());
    }
}
