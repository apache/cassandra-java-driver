package com.datastax.driver.core;

public class QueryOptions {

    protected final ConsistencyLevel consistency;

    /**
     * Creates a new query options object with default consistency level
     * (ConsistencyLevel.ONE).
     */
    public QueryOptions() {
        this(null);
    }

    /**
     * Creates a new query options ojbect using the provided consistency.
     *
     * @param consistency the consistency level to use for the query. If {@code
     * null} is provided and the request requires a consistency level,
     * ConsistencyLevel.ONE is used.
     */
    public QueryOptions(ConsistencyLevel consistency) {
        this.consistency = consistency;
    }

    /**
     * The consistency level.
     *
     * @return the consistency level. Returns {@code null} if no consistency
     * level has been specified.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }
}
