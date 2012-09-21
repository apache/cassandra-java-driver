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

    public ConsistencyLevel consistency() {
        return consistency;
    }

    public int requiredReplicas() {
        return required;
    }

    public int aliveReplicas() {
        return alive;
    }
}
