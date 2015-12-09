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

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import java.util.Map;

/**
 * Versions of the native protocol supported by the driver.
 */
public enum ProtocolVersion {

    V1("1.2.0", 1),
    V2("2.0.0", 2),
    V3("2.1.0", 3),
    V4("2.2.0", 4);

    /**
     * The most recent protocol version supported by the driver.
     */
    public static final ProtocolVersion NEWEST_SUPPORTED = V4;

    private final VersionNumber minCassandraVersion;
    private final int asInt;

    private ProtocolVersion(String minCassandraVersion, int asInt) {
        this.minCassandraVersion = VersionNumber.parse(minCassandraVersion);
        this.asInt = asInt;
    }

    boolean isSupportedBy(Host host) {
        return host.getCassandraVersion() == null ||
                isSupportedBy(host.getCassandraVersion());
    }

    VersionNumber minCassandraVersion() {
        return minCassandraVersion;
    }

    private boolean isSupportedBy(VersionNumber cassandraVersion) {
        return minCassandraVersion.compareTo(cassandraVersion.nextStable()) <= 0;
    }

    DriverInternalError unsupported() {
        return new DriverInternalError("Unsupported protocol version " + this);
    }

    /**
     * Returns the version as an integer.
     *
     * @return the integer representation.
     */
    public int toInt() {
        return asInt;
    }

    private static final Map<Integer, ProtocolVersion> INT_TO_VERSION;

    static {
        Builder<Integer, ProtocolVersion> builder = ImmutableMap.builder();
        for (ProtocolVersion version : values()) {
            builder.put(version.asInt, version);
        }
        INT_TO_VERSION = builder.build();
    }

    /**
     * Returns the value matching an integer version.
     *
     * @param i the version as an integer.
     * @return the matching enum value.
     * @throws IllegalArgumentException if the argument doesn't match any known version.
     */
    public static ProtocolVersion fromInt(int i) {
        ProtocolVersion version = INT_TO_VERSION.get(i);
        if (version == null)
            throw new IllegalArgumentException("No protocol version matching integer version " + i);
        return version;
    }
}
