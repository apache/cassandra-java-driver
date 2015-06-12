/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import com.datastax.driver.core.exceptions.PagingStateException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;

/**
 * An executable query.
 * <p>
 * This represents either a {@link RegularStatement}, a {@link BoundStatement} or a
 * {@link BatchStatement} along with the querying options (consistency level,
 * whether to trace the query, ...).
 */
public abstract class Statement {

    // An exception to the RegularStatement, BoundStatement or BatchStatement rule above. This is
    // used when preparing a statement and for other internal queries. Do not expose publicly.
    static final Statement DEFAULT = new Statement() {
        @Override
        public ByteBuffer getRoutingKey() {
            return null;
        }

        @Override
        public String getKeyspace() {
            return null;
        }
    };

    private volatile ConsistencyLevel consistency;
    private volatile ConsistencyLevel serialConsistency;
    private volatile boolean traceQuery;
    private volatile int fetchSize;
    private volatile RetryPolicy retryPolicy;
    private volatile ByteBuffer pagingState;
    protected volatile Boolean idempotent;

    // We don't want to expose the constructor, because the code relies on this being only sub-classed by RegularStatement, BoundStatement and BatchStatement
    Statement() {
    }

    /**
     * Sets the consistency level for the query.
     *
     * @param consistency the consistency level to set.
     * @return this {@code Statement} object.
     */
    public Statement setConsistencyLevel(ConsistencyLevel consistency) {
        this.consistency = consistency;
        return this;
    }

    /**
     * The consistency level for this query.
     *
     * @return the consistency level for this query, or {@code null} if no
     * consistency level has been specified (through {@code setConsistencyLevel}).
     * In the latter case, the default consistency level will be used.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }

    /**
     * Sets the serial consistency level for the query.
     *
     * The serial consistency level is only used by conditional updates (so INSERT, UPDATE
     * and DELETE with an IF condition). For those, the serial consistency level defines
     * the consistency level of the serial phase (or "paxos" phase) while the
     * normal consistency level defines the consistency for the "learn" phase, i.e. what
     * type of reads will be guaranteed to see the update right away. For instance, if
     * a conditional write has a regular consistency of QUORUM (and is successful), then a
     * QUORUM read is guaranteed to see that write. But if the regular consistency of that
     * write is ANY, then only a read with a consistency of SERIAL is guaranteed to see it
     * (even a read with consistency ALL is not guaranteed to be enough).
     * <p>
     * The serial consistency can only be one of {@code ConsistencyLevel.SERIAL} or
     * {@code ConsistencyLevel.LOCAL_SERIAL}. While {@code ConsistencyLevel.SERIAL} guarantees full
     * linearizability (with other SERIAL updates), {@code ConsistencyLevel.LOCAL_SERIAL} only
     * guarantees it in the local data center.
     * <p>
     * The serial consistency level is ignored for any query that is not a conditional
     * update (serial reads should use the regular consistency level for instance).
     *
     * @param serialConsistency the serial consistency level to set.
     * @return this {@code Statement} object.
     *
     * @throws IllegalArgumentException if {@code serialConsistency} is not one of
     * {@code ConsistencyLevel.SERIAL} or {@code ConsistencyLevel.LOCAL_SERIAL}.
     */
    public Statement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
        if (serialConsistency != ConsistencyLevel.SERIAL && serialConsistency != ConsistencyLevel.LOCAL_SERIAL)
            throw new IllegalArgumentException();
        this.serialConsistency = serialConsistency;
        return this;
    }

    /**
     * The serial consistency level for this query.
     * <p>
     * See {@link #setSerialConsistencyLevel} for more detail on the serial consistency level.
     *
     * @return the consistency level for this query, or {@code null} if no serial
     * consistency level has been specified (through {@code setSerialConsistencyLevel}).
     * In the latter case, the default serial consistency level will be used.
     */
    public ConsistencyLevel getSerialConsistencyLevel() {
        return serialConsistency;
    }

    /**
     * Enables tracing for this query.
     *
     * By default (that is unless you call this method), tracing is not enabled.
     *
     * @return this {@code Statement} object.
     */
    public Statement enableTracing() {
        this.traceQuery = true;
        return this;
    }

    /**
     * Disables tracing for this query.
     *
     * @return this {@code Statement} object.
     */
    public Statement disableTracing() {
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
     * so this method can always return {@code null}. Firstly, some queries do
     * not operate inside a keyspace: keyspace creation, {@code USE} queries,
     * user creation, etc. Secondly, even query that operate within a keyspace
     * do not have to specify said keyspace directly, in which case the
     * currently logged in keyspace (the one set through a {@code USE} query
     * (or through the use of {@link Cluster#connect(String)})). Lastly, as
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
     * @return this {@code Statement} object.
     */
    public Statement setRetryPolicy(RetryPolicy policy) {
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
     * Only {@code SELECT} queries only ever make use of that setting.
     * <p>
     * Note: Paging is not supported with the native protocol version 1. If
     * you call this method with {@code fetchSize &gt; 0} and
     * {@code fetchSize != Integer.MAX_VALUE} and the protocol version is in
     * use (i.e. if you've force version 1 through {@link Cluster.Builder#withProtocolVersion}
     * or you use Cassandra 1.2), you will get {@link UnsupportedProtocolVersionException}
     * when submitting this statement for execution.
     *
     * @param fetchSize the fetch size to use. If {@code fetchSize &lte; 0},
     * the default fetch size will be used. To disable paging of the
     * result set, use {@code fetchSize == Integer.MAX_VALUE}.
     * @return this {@code Statement} object.
     */
    public Statement setFetchSize(int fetchSize) {
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

    /**
     * Sets the paging state.
     * <p>
     * This will cause the next execution of this statement to fetch results from a given
     * page, rather than restarting from the beginning.
     * <p>
     * You get the paging state from a previous execution of the statement (see
     * {@link ExecutionInfo#getPagingState()}.
     * This is typically used to iterate in a "stateless" manner (e.g. across HTTP requests):
     * <pre>
     * {@code
     * Statement st = new SimpleStatement("your query");
     * ResultSet rs = session.execute(st.setFetchSize(20));
     * int available = rs.getAvailableWithoutFetching();
     * for (int i = 0; i < available; i++) {
     *     Row row = rs.one();
     *     // Do something with row (e.g. display it to the user...)
     * }
     * // Get state and serialize as string or byte[] to store it for the next execution
     * // (e.g. pass it as a parameter in the "next page" URI)
     * PagingState pagingState = rs.getExecutionInfo().getPagingState();
     * String savedState = pagingState.toString();
     *
     * // Next execution:
     * // Get serialized state back (e.g. get URI parameter)
     * String savedState = ...
     * Statement st = new SimpleStatement("your query");
     * st.setPagingState(PagingState.fromString(savedState));
     * ResultSet rs = session.execute(st.setFetchSize(20));
     * int available = rs.getAvailableWithoutFetching();
     * for (int i = 0; i < available; i++) {
     *     ...
     * }
     * }
     * </pre>
     * <p>
     * The paging state can only be reused between perfectly identical statements
     * (same query string, same bound parameters). Altering the contents of the paging state
     * or trying to set it on a different statement will cause this method to fail.
     * <p>
     * Note that, due to internal implementation details, the paging state is not portable
     * across native protocol versions (see the
     * <a href="http://datastax.github.io/java-driver/features/native_protocol">online documentation</a>
     * for more explanations about the native protocol).
     * This means that {@code PagingState} instances generated with an old version won't work
     * with a higher version. If that is a problem for you, consider using the "unsafe" API (see
     * {@link #setPagingStateUnsafe(byte[])}).
     *
     * @param pagingState the paging state to set, or {@code null} to remove any state that was
     *                    previously set on this statement.
     * @return this {@code Statement} object.
     *
     * @throws PagingStateException if the paging state does not match this statement.
     */
    public Statement setPagingState(PagingState pagingState) {
        if (this instanceof BatchStatement) {
            throw new UnsupportedOperationException("Cannot set the paging state on a batch statement");
        } else {
            if (pagingState == null) {
                this.pagingState = null;
            } else if (pagingState.matches(this)) {
                this.pagingState = pagingState.getRawState();
            } else {
                throw new PagingStateException("Paging state mismatch, "
                    + "this means that either the paging state contents were altered, "
                    + "or you're trying to apply it to a different statement");
            }
        }
        return this;
    }

    /**
     * Sets the paging state.
     * <p>
     * Contrary to {@link #setPagingState(PagingState)}, this method takes the "raw" form of the
     * paging state (previously extracted with {@link ExecutionInfo#getPagingStateUnsafe()}.
     * It won't validate that this statement matches the one that the paging state was extracted from.
     * If the paging state was altered in any way, you will get unpredictable behavior from
     * Cassandra (ranging from wrong results to a query failure). If you decide to use this variant,
     * it is strongly recommended to add your own validation (for example, signing the raw state with
     * a private key).
     *
     * @param pagingState the paging state to set, or {@code null} to remove any state that was
     *                    previously set on this statement.
     * @return this {@code Statement} object.
     */
    public Statement setPagingStateUnsafe(byte[] pagingState) {
        if (pagingState == null) {
            this.pagingState = null;
        } else {
            this.pagingState = ByteBuffer.wrap(pagingState);
        }
        return this;
    }

    ByteBuffer getPagingState() {
        return pagingState;
    }

    /**
     * Sets whether this statement is idempotent.
     * <p>
     * See {@link #isIdempotent()} for more explanations about this property.
     *
     * @param idempotent the new value.
     * @return this {@code Statement} object.
     */
    public Statement setIdempotent(boolean idempotent) {
        this.idempotent = idempotent;
        return this;
    }

    /**
     * Whether this statement is idempotent, i.e. whether it can be applied multiple times
     * without changing the result beyond the initial application.
     * <p>
     * Idempotence plays a role in {@link com.datastax.driver.core.policies.SpeculativeExecutionPolicy speculative executions}.
     * If a statement is <em>not idempotent</em>, the driver will not schedule speculative
     * executions for it.
     * <p>
     * Note that this method can return {@code null}, in which case the driver will default to
     * {@link QueryOptions#getDefaultIdempotence()}.
     * <p>
     * By default, this method returns {@code null} for all statements, except for
     * {@link BuiltStatement}s, where the value will be inferred from the query: if it updates
     * counters or prepends/appends to a list, the result will be {@code false}, otherwise it
     * will be {@code true}. In all cases, calling {@link #setIdempotent(boolean)} forces a
     * value that overrides every other mechanism.
     *
     * @return whether this statement is idempotent, or {@code null} to use
     * {@link QueryOptions#getDefaultIdempotence()}.
     */
    public Boolean isIdempotent() {
        return idempotent;
    }

    boolean isIdempotentWithDefault(QueryOptions queryOptions) {
        Boolean myValue = this.isIdempotent();
        if (myValue != null)
            return myValue;
        else
            return queryOptions.getDefaultIdempotence();
    }
}
