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

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Indicates that a connection has run out of stream IDs.
 */
public class BusyConnectionException extends DriverException implements CoordinatorException {

    private static final long serialVersionUID = 0;

    private final InetSocketAddress address;

    public BusyConnectionException(InetSocketAddress address) {
        super(String.format("[%s] Connection has run out of stream IDs", address.getAddress()));
        this.address = address;
    }

    public BusyConnectionException(InetSocketAddress address, Throwable cause) {
        super(String.format("[%s] Connection has run out of stream IDs", address.getAddress()), cause);
        this.address = address;
    }

    @Override
    public InetAddress getHost() {
        return address.getAddress();
    }

    @Override
    public InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public BusyConnectionException copy() {
        return new BusyConnectionException(address, this);
    }

}
