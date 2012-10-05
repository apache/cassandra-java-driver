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

    /**
     * The consistency level of the operation that time outed.
     *
     * @return the consistency level of the operation that time outed.
     */
    public ConsistencyLevel consistencyLevel() {
        return consistency;
    }

    /**
     * The number of replica that had acknowledged/responded to the operation
     * before it time outed.
     *
     * @return the number of replica that had acknowledged/responded the
     * operation before it time outed.
     */
    public int receivedAcknowledgements() {
        return received;
    }

    /**
     * The minimum number of replica acknowledgements/responses that were
     * required to fulfill the operation.
     *
     * @return The minimum number of replica acknowledgements/response that
     * were required to fulfill the operation.
     */
    public int requiredAcknowledgements() {
        return required;
    }
}
