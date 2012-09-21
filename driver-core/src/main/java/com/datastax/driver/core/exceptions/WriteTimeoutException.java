package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * A Cassandra timeout during a write query.
 */
public class WriteTimeoutException extends QueryTimeoutException {

    public WriteTimeoutException(ConsistencyLevel consistency, int received, int required) {
        super(String.format("Cassandra timeout during write query at consitency %s (%d replica acknowledged the write over %d required)", consistency, received, required),
              consistency,
              received,
              required);
    }
}
