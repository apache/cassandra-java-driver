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

public enum ConsistencyLevel {

    ANY(0),
    ONE(1),
    TWO(2),
    THREE(3),
    QUORUM(4),
    ALL(5),
    LOCAL_QUORUM(6),
    EACH_QUORUM(7),
    SERIAL(8),
    LOCAL_SERIAL(9),
    LOCAL_ONE(10);

    // Used by the native protocol
    final int code;
    private static final ConsistencyLevel[] codeIdx;

    static {
        int maxCode = -1;
        for (ConsistencyLevel cl : ConsistencyLevel.values())
            maxCode = Math.max(maxCode, cl.code);
        codeIdx = new ConsistencyLevel[maxCode + 1];
        for (ConsistencyLevel cl : ConsistencyLevel.values()) {
            if (codeIdx[cl.code] != null)
                throw new IllegalStateException("Duplicate code");
            codeIdx[cl.code] = cl;
        }
    }

    private ConsistencyLevel(int code) {
        this.code = code;
    }

    static ConsistencyLevel fromCode(int code) {
        if (code < 0 || code >= codeIdx.length)
            throw new DriverInternalError(String.format("Unknown code %d for a consistency level", code));
        return codeIdx[code];
    }

    /**
     * Whether or not the the consistency level applies to the local data-center only.
     *
     * @return whether this consistency level is {@code LOCAL_ONE} or {@code LOCAL_QUORUM}.
     */
    public boolean isDCLocal() {
        return this == LOCAL_ONE || this == LOCAL_QUORUM;
    }
}
