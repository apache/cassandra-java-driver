package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * Exception thrown when the coordinator knows there is not enough replica
 * alive to perform a query with the requested consistency level.
 */
public class UnavailableException extends QueryExecutionException {

    private final ConsistencyLevel consistency;
    private final int required;
    private final int alive;

    public UnavailableException(ConsistencyLevel consistency, int required, int alive) {
        super(String.format("Not enough replica available for query at consistency %s (%d required but only %d alive)", consistency, required, alive));
        this.consistency = consistency;
        this.required = required;
        this.alive = alive;
    }

    /**
     * The consistency level of the operation triggering this unavailable exception.
     *
     * @return the consistency level of the operation triggering this unavailable exception.
     */
    public ConsistencyLevel consistency() {
        return consistency;
    }

    /**
     * The number of replica acknowledgements/responses required to perform the
     * operation (with its required consistency level).
     *
     * @return the number of replica acknowledgements/responses required to perform the
     * operation.
     */
    public int requiredReplicas() {
        return required;
    }

    /**
     * The number of replica that were known to be alive by the Cassandra
     * coordinator node when it tried to execute the operation.
     *
     * @return The number of replica that were known to be alive by the Cassandra
     * coordinator node when it tried to execute the operation.
     */
    public int aliveReplicas() {
        return alive;
    }
}
