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

    public static ConsistencyLevel from(org.apache.cassandra.db.ConsistencyLevel cl) {
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
}
