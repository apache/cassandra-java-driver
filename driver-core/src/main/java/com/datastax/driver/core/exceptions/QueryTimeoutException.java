package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * A Cassandra timeout during a query.
 *
 * Such an exception is returned when the query has been tried by Cassandra but
 * cannot be achieved with the requested consistency level within the rpc
 * timeout set for Cassandra.
 */
public class QueryTimeoutException extends QueryExecutionException {

    private final ConsistencyLevel consistency;
    private final int received;
    private final int required;

    protected QueryTimeoutException(String msg, ConsistencyLevel consistency, int received, int required) {
        super(msg);
        this.consistency = consistency;
        this.received = received;
        this.required = required;
    }

    public ConsistencyLevel consistencyLevel() {
        return consistency;
    }

    public int receivedAcknowledgements() {
        return received;
    }

    public int requiredAcknowledgements() {
        return required;
    }
}
