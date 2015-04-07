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
package com.datastax.driver.core;

import java.net.InetSocketAddress;

/**
 * Indicates that we've attempted to connect to a 1.2 C* node with version 2 of
 * the protocol.
 */
class UnsupportedProtocolVersionException extends Exception {

    private static final long serialVersionUID = 0;

    public final InetSocketAddress address;
    public final int versionUnsupported;

    public UnsupportedProtocolVersionException(InetSocketAddress address, int versionUnsupported)
    {
        super(String.format("[%s] Host %s does not support protocol version %d", address, address, versionUnsupported));
        this.address = address;
        this.versionUnsupported = versionUnsupported;
    }
}
