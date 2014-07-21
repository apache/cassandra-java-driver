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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.DriverInternalError;

/**
 * Versions of the native protocol supported by the driver.
 */
public enum ProtocolVersion {

    // NB: order of the constants matters
    V1("1.2.0"),
    V2("2.0.0"),
    V3("2.1.0-rc1"),
    ;

    public static ProtocolVersion NEWEST_SUPPORTED = V3;

    private final VersionNumber minCassandraVersion;

    private ProtocolVersion(String minCassandraVersion) {
        this.minCassandraVersion = VersionNumber.parse(minCassandraVersion);
    }

    // We might want to expose it publicly?
    boolean isSupportedBy(Host host) {
        return host.getCassandraVersion() == null ||
               isSupportedBy(host.getCassandraVersion());
    }

    VersionNumber minCassandraVersion() {
        return minCassandraVersion;
    }

    private boolean isSupportedBy(VersionNumber cassandraVersion) {
        return minCassandraVersion.compareTo(cassandraVersion) <= 0;
    }

    DriverInternalError unsupported() {
        return new DriverInternalError("Unsupported protocol version " + this);
    }

    int toInt() {
        return ordinal() + 1;
    }

    public static ProtocolVersion fromInt(int i) {
        ProtocolVersion[] versions = values();
        int ordinal = i - 1;

        if (ordinal < 0 || ordinal >= versions.length)
            throw new DriverInternalError(String.format("Cannot deduce protocol version from %d (highest supported version is %s)",
                                                        i, versions[versions.length - 1]));
        return versions[i - 1];
    }
}
