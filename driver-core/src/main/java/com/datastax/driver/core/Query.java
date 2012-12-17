package com.datastax.driver.core;

import java.nio.ByteBuffer;

import com.datastax.driver.core.policies.RetryPolicy;

/**
 * An executable query.
 * <p>
 * This represents either a {@link Statement} or a {@link BoundStatement}
 * along with the query options (consistency level, whether to trace the query, ...).
 */
public abstract class Query {

    // An exception to the Statement or BoundStatement rule above. This is
    // used when preparing a statement and for other internal queries. Do not expose publicly.
    static final Query DEFAULT = new Query() { public ByteBuffer getRoutingKey() { return null; } };

    private volatile ConsistencyLevel consistency;
    private volatile boolean traceQuery;

    private volatile RetryPolicy retryPolicy;

    // We don't want to expose the constructor, because the code rely on this being only subclassed by Statement and BoundStatement
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

    /**
     * The routing key (in binary raw form) to use for token aware routing of this query.
     * <p>
     * The routing key is optional in the sense that implementers are free to
     * return {@code null}. The routing key is an hint used for token aware routing (see
     * {@link com.datastax.driver.core.policies.TokenAwarePolicy}), and
     * if provided should correspond to the binary value for the query
     * partition key. However, not providing a routing key never causes a query
     * to fail and if the load balancing policy used is not token aware, then
     * the routing key can be safely ignored.
     *
     * @return the routing key for this query or {@code null}.
     */
    public abstract ByteBuffer getRoutingKey();

    /**
     * Sets the retry policy to use for this query.
     * <p>
     * The default retry policy, if this method is not called, is the one returned by
     * {@link Policies#getRetryPolicy} in the cluster configuration. This
     * method is thus only useful in case you want to punctually override the
     * default policy for this request.
     *
     * @param policy the retry policy to use for this query.
     * @return this {@code Query} object.
     */
    public Query setRetryPolicy(RetryPolicy policy) {
        this.retryPolicy = policy;
        return this;
    }

    /**
     * The retry policy sets for this query, if any.
     *
     * @return the retry policy sets specifically for this query or {@code null} if no query specific
     * retry policy has been set through {@link #setRetryPolicy} (in which case
     * the Cluster retry policy will apply if necessary).
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }
}
