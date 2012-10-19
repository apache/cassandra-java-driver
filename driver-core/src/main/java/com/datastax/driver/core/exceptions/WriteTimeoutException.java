package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.WriteType;

/**
 * A Cassandra timeout during a write query.
 */
public class WriteTimeoutException extends QueryTimeoutException {

    private final WriteType writeType;

    public WriteTimeoutException(ConsistencyLevel consistency, WriteType writeType, int received, int required) {
        super(String.format("Cassandra timeout during write query at consitency %s (%d replica acknowledged the write over %d required)", consistency, received, required),
              consistency,
              received,
              required);
        this.writeType = writeType;
    }

    /**
     * The type of the write for which a timeout was raised.
     *
     * @return the type of the write for which a timeout was raised.
     */
    public WriteType getWriteType() {
        return writeType;
    }
}
