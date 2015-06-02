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

import java.net.InetSocketAddress;

/**
 * Indicates that the contacted host replied with an UNPREPARED error code.
 * This class is mainly intended for internal use;
 * client applications are not expected to deal with this exception directly,
 * because the driver would transparently prepare the query and execute it again;
 * but such exceptions are likely to appear occasionally in the driver logs.
 */
public class UnpreparedException extends DriverInternalError {

    private static final long serialVersionUID = 0;

    private final InetSocketAddress address;

    public UnpreparedException(InetSocketAddress address, String message) {
        super(String.format("A prepared query was submitted on %s but was not known of that node: %s", address, message));
        this.address = address;
    }

    /**
     * Private constructor used solely when copying exceptions.
     */
    private UnpreparedException(InetSocketAddress address, String message, UnpreparedException cause) {
        super(message, cause);
        this.address = address;
    }

    /**
     * The full address of the host that replied with an UNPREPARED error code.
     *
     * @return The full address of the host that replied with an UNPREPARED error code.
     */
    public InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public UnpreparedException copy() {
        return new UnpreparedException(address, getMessage(), this);
    }
}
