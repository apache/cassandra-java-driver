/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.net.InetSocketAddress;

/**
 * Indicates that the contacted host was bootstrapping.
 * This class is mainly intended for internal use;
 * client applications are not expected to deal with this exception directly,
 * because the driver would transparently retry the same query on another host;
 * but such exceptions are likely to appear occasionally in the driver logs.
 */
public class BootstrappingException extends DriverInternalError {

    private static final long serialVersionUID = 0;

    private final InetSocketAddress address;

    public BootstrappingException(InetSocketAddress address, String message) {
        super(String.format("Queried host (%s) was bootstrapping: %s", address, message));
        this.address = address;
    }

    /**
     * Private constructor used solely when copying exceptions.
     */
    private BootstrappingException(InetSocketAddress address, String message, BootstrappingException cause) {
        super(message, cause);
        this.address = address;
    }

    /**
     * The full address of the host that was bootstrapping.
     *
     * @return The full address of the host that was bootstrapping.
     */
    public InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public BootstrappingException copy() {
        return new BootstrappingException(address, getMessage(), this);
    }
}
