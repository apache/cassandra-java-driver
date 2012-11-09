package com.datastax.driver.core.exceptions;

import java.net.InetAddress;

/**
 * Indicates an error during the authentication phase while connecting to a node.
 */
public class AuthenticationException extends DriverUncheckedException {

    private final InetAddress host;

    public AuthenticationException(InetAddress host, String message) {
        super(String.format("Authentication error on host %s: %s", host, message));
        this.host = host;
    }

    /**
     * The host for which the authentication failed.
     *
     * @return the host for which the authentication failed.
     */
    public InetAddress getHost() {
        return host;
    }
}
