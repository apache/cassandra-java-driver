/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import com.datastax.driver.core.ProtocolVersion;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Indicates that we've attempted to connect to a Cassandra node with a protocol version
 * that it cannot handle (e.g., connecting to a C* 1.2 node with protocol version 2).
 */
public class UnsupportedProtocolVersionException extends DriverException implements CoordinatorException {

    private static final long serialVersionUID = 0;

    private final InetSocketAddress address;

    private final ProtocolVersion unsupportedVersion;

    private final ProtocolVersion serverVersion;

    public UnsupportedProtocolVersionException(InetSocketAddress address, ProtocolVersion unsupportedVersion, ProtocolVersion serverVersion) {
        super(String.format("[%s] Host does not support protocol version %s but %s", address, unsupportedVersion, serverVersion));
        this.address = address;
        this.unsupportedVersion = unsupportedVersion;
        this.serverVersion = serverVersion;
    }

    public UnsupportedProtocolVersionException(InetSocketAddress address, ProtocolVersion unsupportedVersion, ProtocolVersion serverVersion, Throwable cause) {
        super(String.format("[%s] Host does not support protocol version %s but %s", address, unsupportedVersion, serverVersion), cause);
        this.address = address;
        this.unsupportedVersion = unsupportedVersion;
        this.serverVersion = serverVersion;
    }

    @Override
    public InetAddress getHost() {
        return address.getAddress();
    }

    @Override
    public InetSocketAddress getAddress() {
        return address;
    }

    public ProtocolVersion getServerVersion() {
        return serverVersion;
    }

    public ProtocolVersion getUnsupportedVersion() {
        return unsupportedVersion;
    }

    @Override
    public UnsupportedProtocolVersionException copy() {
        return new UnsupportedProtocolVersionException(address, unsupportedVersion, serverVersion, this);
    }


}
