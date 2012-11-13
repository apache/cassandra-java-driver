package com.datastax.driver.core;

/**
 * An executable query.
 * <p>
 * This represents either a {@link CQLStatement} or a {@link BoundStatement}
 * along with the query options (consistency level, whether to trace the query, ...).
 */
public abstract class Query {

    // An exception to the CQLStatement or BoundStatement rule above. This is
    // used when preparing a statement and for other internal queries. Do not expose publicly.
    static final Query DEFAULT = new Query() {};

    private volatile ConsistencyLevel consistency;
    private volatile boolean traceQuery;

    // We don't want to expose the constructor, because the code rely on this being only subclassed by CQLStatement and BoundStatement
    Query() {
        this.consistency = ConsistencyLevel.ONE;
    }

    /**
     * Sets the consistency level for the query.
     * <p>
     * The default consistency level, if this method is not called, is ConsistencyLevel.ONE.
     *
     * @param consistency the consistency level to set.
     * @return this {@code Query} object.
     */
    public Query setConsistencyLevel(ConsistencyLevel consistency) {
        this.consistency = consistency;
        return this;
    }

    /**
     * The consistency level.
     *
     * @return the consistency level. Returns {@code ConsistencyLeve.ONE} if no
     * consistency level has been specified.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }

    /**
     * Enable tracing for this query.
     *
     * By default (i.e. unless you call this method), tracing is not enabled.
     *
     * @return this {@code Query} object.
     */
    public Query setTracing() {
        this.traceQuery = true;
        return this;
    }

    /**
     * Disable tracing for this query.
     *
     * @return this {@code Query} object.
     */
    public Query unsetTracing() {
        this.traceQuery = false;
        return this;
    }

    /**
     * Whether tracing is enabled for this query or not.
     *
     * @return {@code true} if this query has tracing enabled, {@code false}
     * otherwise.
     */
    public boolean isTracing() {
        return traceQuery;
    }

}
