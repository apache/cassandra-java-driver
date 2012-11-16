package com.datastax.driver.core;

/**
 * The type of a Cassandra write query.
 *
 * This information is returned by Cassandra when a write timeout is raised to
 * indicate what type of write timeouted. This information is useful to decide
 * which retry policy to adopt.
 */
public enum WriteType
{
    /** A write to a single partition key. Such writes are guaranteed to be atomic and isolated. */
    SIMPLE,
    /** A write to a multiple partition key that used the distributed batch log to ensure atomicity. */
    BATCH,
    /** A write to a multiple partition key that doesn't use the distributed batch log. Atomicity for such writes is not guaranteed */
    UNLOGGED_BATCH,
    /** A counter write (that can be for one or multiple partition key). Such write should not be replayed to avoid overcount. */
    COUNTER,
    /** The initial write to the distributed batch log that Cassandra performs internally before a BATCH write. */
    BATCH_LOG;

    static WriteType from(org.apache.cassandra.db.WriteType writeType) {
        switch (writeType) {
            case SIMPLE: return SIMPLE;
            case BATCH: return BATCH;
            case UNLOGGED_BATCH: return UNLOGGED_BATCH;
            case COUNTER: return COUNTER;
            case BATCH_LOG: return BATCH_LOG;
        }
        throw new AssertionError();
    }

    static org.apache.cassandra.db.WriteType toCassandraWriteType(WriteType writeType) {
        switch (writeType) {
            case SIMPLE: return org.apache.cassandra.db.WriteType.SIMPLE;
            case BATCH: return org.apache.cassandra.db.WriteType.BATCH;
            case UNLOGGED_BATCH: return org.apache.cassandra.db.WriteType.UNLOGGED_BATCH;
            case COUNTER: return org.apache.cassandra.db.WriteType.COUNTER;
            case BATCH_LOG: return org.apache.cassandra.db.WriteType.BATCH_LOG;
        }
        throw new AssertionError();
    }
}
