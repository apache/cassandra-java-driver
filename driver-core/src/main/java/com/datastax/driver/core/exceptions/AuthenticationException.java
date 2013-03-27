/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.exceptions;

import java.net.InetAddress;

/**
 * Indicates an error during the authentication phase while connecting to a node.
 */
public class AuthenticationException extends DriverException {

    private final InetAddress host;

    public AuthenticationException(InetAddress host, String message) {
        super(String.format("Authentication error on host %s: %s", host, message));
        this.host = host;
    }

    private AuthenticationException(String message, Throwable cause, InetAddress host)
    {
        super(message, cause);
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

    public DriverException copy() {
        return new AuthenticationException(getMessage(), this, host);
    }
}
