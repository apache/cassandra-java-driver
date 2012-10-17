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
    EACH_QUORUM;

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
        }
        throw new AssertionError();
    }
}
