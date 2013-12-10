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

public enum ConsistencyLevel
{
    ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_QUORUM,
    EACH_QUORUM,
    LOCAL_ONE;

    static org.apache.cassandra.db.ConsistencyLevel DEFAULT_CASSANDRA_CL = org.apache.cassandra.db.ConsistencyLevel.ONE;

    static ConsistencyLevel from(org.apache.cassandra.db.ConsistencyLevel cl) {
        switch (cl) {
            case ANY: return ANY;
            case ONE: return ONE;
            case TWO: return TWO;
            case THREE: return THREE;
            case QUORUM: return QUORUM;
            case ALL: return ALL;
            case LOCAL_QUORUM: return LOCAL_QUORUM;
            case EACH_QUORUM: return EACH_QUORUM;
            case LOCAL_ONE: return LOCAL_ONE;
        }
        throw new AssertionError();
    }

    static org.apache.cassandra.db.ConsistencyLevel toCassandraCL(ConsistencyLevel cl) {
        if (cl == null)
            return org.apache.cassandra.db.ConsistencyLevel.ONE;

        switch (cl) {
            case ANY: return org.apache.cassandra.db.ConsistencyLevel.ANY;
            case ONE: return org.apache.cassandra.db.ConsistencyLevel.ONE;
            case TWO: return org.apache.cassandra.db.ConsistencyLevel.TWO;
            case THREE: return org.apache.cassandra.db.ConsistencyLevel.THREE;
            case QUORUM: return org.apache.cassandra.db.ConsistencyLevel.QUORUM;
            case ALL: return org.apache.cassandra.db.ConsistencyLevel.ALL;
            case LOCAL_QUORUM: return org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
            case EACH_QUORUM: return org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
            case LOCAL_ONE: return org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
        }
        throw new AssertionError();
    }
}
