/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.nio.ByteBuffer;

import com.datastax.driver.core.policies.RetryPolicy;

/**
 * An executable query.
 * <p>
 * This represents either a {@link Statement}, a {@link BoundStatement} or a
 * {@link BatchStatement} along with the query options (consistency level,
 * whether to trace the query, ...).
 */
public abstract class Query {

    // An exception to the Statement or BoundStatement rule above. This is
    // used when preparing a statement and for other internal queries. Do not expose publicly.
    static final Query DEFAULT = new Query() {
        @Override
        public ByteBuffer getRoutingKey() { return null; }

        @Override
        public String getKeyspace() { return null; }
    };

    // TODO: we'd need to make that default configurable, which involve not making static naymore.
    private static final int DEFAULT_FETCH_SIZE = 5000;

    private volatile ConsistencyLevel consistency;
    private volatile boolean traceQuery;
    private volatile int fetchSize;

    private volatile RetryPolicy retryPolicy;

    // We don't want to expose the constructor, because the code rely on this being only subclassed Statement, BoundStatement and BatchStatement
    Query() {
        this.consistency = ConsistencyLevel.ONE;
        this.fetchSize = DEFAULT_FETCH_SIZE;
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
     * Returns the consistency level.
     *
     * @return the consistency level, which is {@code ConsistencyLevel.ONE} if no
     * consistency level has been specified.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }

    /**
     * Enables tracing for this query.
     *
     * By default (that is unless you call this method), tracing is not enabled.
     *
     * @return this {@code Query} object.
     */
    public Query enableTracing() {
        this.traceQuery = true;
        return this;
    }

    /**
     * Disables tracing for this query.
     *
     * @return this {@code Query} object.
     */
    public Query disableTracing() {
        this.traceQuery = false;
        return this;
    }

    /**
     * Returns whether tracing is enabled for this query or not.
     *
     * @return {@code true} if this query has tracing enabled, {@code false}
     * otherwise.
     */
    public boolean isTracing() {
        return traceQuery;
    }

    /**
     * Returns the routing key (in binary raw form) to use for token aware 
     * routing of this query.
     * <p>
     * The routing key is optional in that implementers are free to
     * return {@code null}. The routing key is an hint used for token-aware routing (see
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
     * Returns the keyspace this query operates on.
     * <p>
     * Note that not all query specify on which keyspace they operate on, and
     * so this method can always reutrn {@code null}. Firstly, some queries do
     * not operate inside a keyspace: keyspace creation, {@code USE} queries,
     * user creation, etc. Secondly, even query that operate within a keyspace
     * do not have to specify said keyspace directly, in which case the
     * currently logged in keyspace (the one set through a {@code USE} query
     * (or through the use of {@link Session#connect(String)})). Lastly, as
     * for the routing key, this keyspace information is only a hint for
     * token-aware routing (since replica placement depend on the replication
     * strategy in use which is a per-keyspace property) and having this method
     * return {@code null} (or even a bogus keyspace name) will never cause the
     * query to fail.
     *
     * @return the keyspace this query operate on if relevant or {@code null}.
     */
    public abstract String getKeyspace();

    /**
     * Sets the retry policy to use for this query.
     * <p>
     * The default retry policy, if this method is not called, is the one returned by
     * {@link com.datastax.driver.core.policies.Policies#getRetryPolicy} in the
     * cluster configuration. This method is thus only useful in case you want
     * to punctually override the default policy for this request.
     *
     * @param policy the retry policy to use for this query.
     * @return this {@code Query} object.
     */
    public Query setRetryPolicy(RetryPolicy policy) {
        this.retryPolicy = policy;
        return this;
    }

    /**
     * Returns the retry policy sets for this query, if any.
     *
     * @return the retry policy sets specifically for this query or {@code null} if no query specific
     * retry policy has been set through {@link #setRetryPolicy} (in which case
     * the Cluster retry policy will apply if necessary).
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * Sets the query fetch size.
     * <p>
     * The fetch size controls how much resulting rows will be retrieved
     * simultaneously (the goal being to avoid loading too much results
     * in memory for queries yielding large results). Please note that
     * while value as low as 1 can be used, it is *highly* discouraged to
     * use such a low value in practice as it will yield very poor
     * performance. If in doubt, leaving the default is probably a good
     * idea.
     * <p>
     * Also note that only {@code SELECT} queries only ever make use of that
     * setting.
     *
     * @param fetchSize the fetch size to use. If {@code fetchSize &gte; 0},
     * the default fetch size will be used. To disable paging of the
     * result set, use {@code fetchSize = Integer.MAX_VALUE}.
     * @return this {@code Query} object.
     */
    public Query setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }


    /**
     * The fetch size for this query.
     *
     * @return the fetch size for this query. If that value is less or equal
     * to 0 (the default unless {@link #setFetchSize} is used), the default
     * fetch size will be used.
     */
    public int getFetchSize() {
        return fetchSize;
    }
}
