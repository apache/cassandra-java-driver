package com.datastax.driver.core;

public class QueryOptions {

    // Don't expose that publicly as this would break if someone uses
    // traceQuery (and don't use internally if traceQuery is set).
    static final QueryOptions DEFAULT = new QueryOptions();

    protected final ConsistencyLevel consistency;
    protected volatile boolean traceQuery;

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

    /**
     * Enable tracing for the query using these options.
     *
     * By default (i.e. unless you call this method), tracing is not enabled.
     *
     * @return this {@code QueryOptions} object.
     */
    public QueryOptions setTracing() {
        traceQuery = true;
        return this;
    }

    /**
     * Disable tracing for the query using these options.
     *
     * @return this {@code QueryOptions} object.
     */
    public QueryOptions unsetTracing() {
        traceQuery = false;
        return this;
    }

    /**
     * Whether to trace the query or not.
     *
     * @return {@code true} if this QueryOptions has query tracing enable,
     * {@code false} otherwise.
     */
    public boolean isTracing() {
        return traceQuery;
    }
}
