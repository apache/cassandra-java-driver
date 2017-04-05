/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.PagingStateException;
import com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * An executable query.
 * <p/>
 * This represents either a {@link RegularStatement}, a {@link BoundStatement} or a
 * {@link BatchStatement} along with the querying options (consistency level,
 * whether to trace the query, ...).
 */
public abstract class Statement {

    /**
     * A special ByteBuffer value that can be used with custom payloads
     * to denote a null value in a payload map.
     */
    public static final ByteBuffer NULL_PAYLOAD_VALUE = ByteBuffer.allocate(0);

    // An exception to the RegularStatement, BoundStatement or BatchStatement rule above. This is
    // used when preparing a statement and for other internal queries. Do not expose publicly.
    static final Statement DEFAULT = new Statement() {
        @Override
        public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
            return null;
        }

        @Override
        public String getKeyspace() {
            return null;
        }

        @Override
        public ConsistencyLevel getConsistencyLevel() {
            return ConsistencyLevel.ONE;
        }
    };

    private volatile ConsistencyLevel consistency;
    private volatile ConsistencyLevel serialConsistency;
    private volatile boolean traceQuery;
    private volatile int fetchSize;
    private volatile long defaultTimestamp = Long.MIN_VALUE;
    private volatile int readTimeoutMillis = Integer.MIN_VALUE;
    private volatile RetryPolicy retryPolicy;
    private volatile ByteBuffer pagingState;
    protected volatile Boolean idempotent;
    private volatile Map<String, ByteBuffer> outgoingPayload;

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
     * <p/>
     * The serial consistency level is only used by conditional updates ({@code INSERT}, {@code UPDATE}
     * or {@code DELETE} statements with an {@code IF} condition).
     * For those, the serial consistency level defines
     * the consistency level of the serial phase (or "paxos" phase) while the
     * normal consistency level defines the consistency for the "learn" phase, i.e. what
     * type of reads will be guaranteed to see the update right away. For instance, if
     * a conditional write has a regular consistency of QUORUM (and is successful), then a
     * QUORUM read is guaranteed to see that write. But if the regular consistency of that
     * write is ANY, then only a read with a consistency of SERIAL is guaranteed to see it
     * (even a read with consistency ALL is not guaranteed to be enough).
     * <p/>
     * The serial consistency can only be one of {@code ConsistencyLevel.SERIAL} or
     * {@code ConsistencyLevel.LOCAL_SERIAL}. While {@code ConsistencyLevel.SERIAL} guarantees full
     * linearizability (with other SERIAL updates), {@code ConsistencyLevel.LOCAL_SERIAL} only
     * guarantees it in the local data center.
     * <p/>
     * The serial consistency level is ignored for any query that is not a conditional
     * update (serial reads should use the regular consistency level for instance).
     *
     * @param serialConsistency the serial consistency level to set.
     * @return this {@code Statement} object.
     * @throws IllegalArgumentException if {@code serialConsistency} is not one of
     *                                  {@code ConsistencyLevel.SERIAL} or {@code ConsistencyLevel.LOCAL_SERIAL}.
     */
    public Statement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
        if (!serialConsistency.isSerial())
            throw new IllegalArgumentException("Supplied consistency level is not serial: " + serialConsistency);
        this.serialConsistency = serialConsistency;
        return this;
    }

    /**
     * The serial consistency level for this query.
     * <p/>
     * See {@link #setSerialConsistencyLevel(ConsistencyLevel)} for more detail on the serial consistency level.
     *
     * @return the serial consistency level for this query, or {@code null} if no serial
     * consistency level has been specified (through {@link #setSerialConsistencyLevel(ConsistencyLevel)}).
     * In the latter case, the default serial consistency level will be used.
     */
    public ConsistencyLevel getSerialConsistencyLevel() {
        return serialConsistency;
    }

    /**
     * Enables tracing for this query.
     * <p/>
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
     * <p/>
     * The routing key is optional in that implementers are free to
     * return {@code null}. The routing key is an hint used for token-aware routing (see
     * {@link com.datastax.driver.core.policies.TokenAwarePolicy}), and
     * if provided should correspond to the binary value for the query
     * partition key. However, not providing a routing key never causes a query
     * to fail and if the load balancing policy used is not token aware, then
     * the routing key can be safely ignored.
     *
     * @param protocolVersion the protocol version that will be used if the actual
     *                        implementation needs to serialize something to compute
     *                        the key.
     * @param codecRegistry   the codec registry that will be used if the actual
     *                        implementation needs to serialize something to compute
     *                        this key.
     * @return the routing key for this query or {@code null}.
     */
    public abstract ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry);

    /**
     * Returns the keyspace this query operates on.
     * <p/>
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
     * <p/>
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
     * <p/>
     * The fetch size controls how much resulting rows will be retrieved
     * simultaneously (the goal being to avoid loading too much results
     * in memory for queries yielding large results). Please note that
     * while value as low as 1 can be used, it is *highly* discouraged to
     * use such a low value in practice as it will yield very poor
     * performance. If in doubt, leaving the default is probably a good
     * idea.
     * <p/>
     * Only {@code SELECT} queries only ever make use of that setting.
     * <p/>
     * Note: Paging is not supported with the native protocol version 1. If
     * you call this method with {@code fetchSize &gt; 0} and
     * {@code fetchSize != Integer.MAX_VALUE} and the protocol version is in
     * use (i.e. if you've force version 1 through {@link Cluster.Builder#withProtocolVersion}
     * or you use Cassandra 1.2), you will get {@link UnsupportedProtocolVersionException}
     * when submitting this statement for execution.
     *
     * @param fetchSize the fetch size to use. If {@code fetchSize &lte; 0},
     *                  the default fetch size will be used. To disable paging of the
     *                  result set, use {@code fetchSize == Integer.MAX_VALUE}.
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
     * Sets the default timestamp for this query (in microseconds since the epoch).
     * <p/>
     * This feature is only available when version {@link ProtocolVersion#V3 V3} or
     * higher of the native protocol is in use. With earlier versions, calling this
     * method has no effect.
     * <p/>
     * The actual timestamp that will be used for this query is, in order of
     * preference:
     * <ul>
     * <li>the timestamp specified directly in the CQL query string (using the
     * {@code USING TIMESTAMP} syntax);</li>
     * <li>the timestamp specified through this method, if different from
     * {@link Long#MIN_VALUE};</li>
     * <li>the timestamp returned by the {@link TimestampGenerator} currently in use,
     * if different from {@link Long#MIN_VALUE}.</li>
     * </ul>
     * If none of these apply, no timestamp will be sent with the query and Cassandra
     * will generate a server-side one (similar to the pre-V3 behavior).
     *
     * @param defaultTimestamp the default timestamp for this query (must be strictly
     *                         positive).
     * @return this {@code Statement} object.
     * @see Cluster.Builder#withTimestampGenerator(TimestampGenerator)
     */
    public Statement setDefaultTimestamp(long defaultTimestamp) {
        this.defaultTimestamp = defaultTimestamp;
        return this;
    }

    /**
     * The default timestamp for this query.
     *
     * @return the default timestamp (in microseconds since the epoch).
     */
    public long getDefaultTimestamp() {
        return defaultTimestamp;
    }

    /**
     * Overrides the default per-host read timeout ({@link SocketOptions#getReadTimeoutMillis()})
     * for this statement.
     * <p/>
     * You should override this only for statements for which the coordinator may allow a longer server-side
     * timeout (for example aggregation queries).
     *
     * @param readTimeoutMillis the timeout to set. Negative values are not allowed. If it is 0, the read timeout will
     *                          be disabled for this statement.
     * @return this {@code Statement} object.
     */
    public Statement setReadTimeoutMillis(int readTimeoutMillis) {
        Preconditions.checkArgument(readTimeoutMillis >= 0, "read timeout must be >= 0");
        this.readTimeoutMillis = readTimeoutMillis;
        return this;
    }

    /**
     * Return the per-host read timeout that was set for this statement.
     *
     * @return the timeout. Note that a negative value means that the default
     * {@link SocketOptions#getReadTimeoutMillis()} will be used.
     */
    public int getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    /**
     * Sets the paging state.
     * <p/>
     * This will cause the next execution of this statement to fetch results from a given
     * page, rather than restarting from the beginning.
     * <p/>
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
     * <p/>
     * The paging state can only be reused between perfectly identical statements
     * (same query string, same bound parameters). Altering the contents of the paging state
     * or trying to set it on a different statement will cause this method to fail.
     * <p/>
     * Note that, due to internal implementation details, the paging state is not portable
     * across native protocol versions (see the
     * <a href="http://datastax.github.io/java-driver/features/native_protocol">online documentation</a>
     * for more explanations about the native protocol).
     * This means that {@code PagingState} instances generated with an old version won't work
     * with a higher version. If that is a problem for you, consider using the "unsafe" API (see
     * {@link #setPagingStateUnsafe(byte[])}).
     *
     * @param pagingState   the paging state to set, or {@code null} to remove any state that was
     *                      previously set on this statement.
     * @param codecRegistry the codec registry that will be used if this method needs to serialize the
     *                      statement's values in order to check that the paging state matches.
     * @return this {@code Statement} object.
     * @throws PagingStateException if the paging state does not match this statement.
     * @see #setPagingState(PagingState)
     */
    public Statement setPagingState(PagingState pagingState, CodecRegistry codecRegistry) {
        if (this instanceof BatchStatement) {
            throw new UnsupportedOperationException("Cannot set the paging state on a batch statement");
        } else {
            if (pagingState == null) {
                this.pagingState = null;
            } else if (pagingState.matches(this, codecRegistry)) {
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
     * <p/>
     * This method calls {@link #setPagingState(PagingState, CodecRegistry)} with {@link CodecRegistry#DEFAULT_INSTANCE}.
     * Whether you should use this or the other variant depends on the type of statement this is
     * called on:
     * <ul>
     * <li>for a {@link BoundStatement}, the codec registry isn't actually needed, so it's always safe to
     * use this method;</li>
     * <li>for a {@link SimpleStatement} or {@link BuiltStatement}, you can use this method if you use no
     * custom codecs, or if your custom codecs are registered with the default registry. Otherwise, use
     * the other method and provide the registry that contains your codecs.</li>
     * </ul>
     *
     * @param pagingState the paging state to set, or {@code null} to remove any state that was
     *                    previously set on this statement.
     */
    public Statement setPagingState(PagingState pagingState) {
        return setPagingState(pagingState, CodecRegistry.DEFAULT_INSTANCE);
    }

    /**
     * Sets the paging state.
     * <p/>
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
     * <p/>
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
     * <p/>
     * If a statement is <em>not idempotent</em>, the driver will ensure that it never gets executed more than once,
     * which means:
     * <ul>
     * <li>avoiding {@link RetryPolicy retries} on write timeouts or request errors;</li>
     * <li>never scheduling {@link com.datastax.driver.core.policies.SpeculativeExecutionPolicy speculative executions}.
     * </li>
     * </ul>
     * (this behavior is implemented in the driver internals, the corresponding policies will not even be invoked).
     * <p/>
     * Note that this method can return {@code null}, in which case the driver will default to
     * {@link QueryOptions#getDefaultIdempotence()}.
     * <p/>
     * By default, this method returns {@code null} for all statements, except for
     * <ul>
     * <li>{@link BuiltStatement} - value will be inferred  from the query: if it updates counters,
     * prepends/appends to a list, or uses a function call or
     * {@link com.datastax.driver.core.querybuilder.QueryBuilder#raw(String)} anywhere in an inserted value,
     * the result will be {@code false}; otherwise it will be {@code true}.
     * </li>
     * <li>
     * {@link com.datastax.driver.core.querybuilder.Batch} and {@link BatchStatement}:
     * <ol>
     * <li>If any statement in batch has isIdempotent() false - return false</li>
     * <li>If no statements with isIdempotent() false, but some have isIdempotent() null - return null</li>
     * <li>Otherwise - return true</li>
     * </ol>
     * </li>
     * </ul>
     * In all cases, calling {@link #setIdempotent(boolean)} forces a value that overrides calculated value.
     * <p/>
     * Note that when a statement is prepared ({@link Session#prepare(String)}), its idempotence flag will be propagated
     * to all {@link PreparedStatement}s created from it.
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

    /**
     * Returns this statement's outgoing payload.
     * Each time this statement is executed, this payload will be included in the query request.
     * <p/>
     * This method returns {@code null} if no payload has been set, otherwise
     * it always returns immutable maps.
     * <p/>
     * This feature is only available with {@link ProtocolVersion#V4} or above.
     * Trying to include custom payloads in requests sent by the driver
     * under lower protocol versions will result in an
     * {@link com.datastax.driver.core.exceptions.UnsupportedFeatureException}
     * (wrapped in a {@link com.datastax.driver.core.exceptions.NoHostAvailableException}).
     *
     * @return the outgoing payload to include with this statement,
     * or {@code null} if no payload has been set.
     * @since 2.2
     */
    public Map<String, ByteBuffer> getOutgoingPayload() {
        return outgoingPayload;
    }

    /**
     * Set the given outgoing payload on this statement.
     * Each time this statement is executed, this payload will be included in the query request.
     * <p/>
     * This method makes a defensive copy of the given map, but its values
     * remain inherently mutable. Care should be taken not to modify the original map
     * once it is passed to this method.
     * <p/>
     * This feature is only available with {@link ProtocolVersion#V4} or above.
     * Trying to include custom payloads in requests sent by the driver
     * under lower protocol versions will result in an
     * {@link com.datastax.driver.core.exceptions.UnsupportedFeatureException}
     * (wrapped in a {@link com.datastax.driver.core.exceptions.NoHostAvailableException}).
     *
     * @param payload the outgoing payload to include with this statement,
     *                or {@code null} to clear any previously entered payload.
     * @return this {@link Statement} object.
     * @since 2.2
     */
    public Statement setOutgoingPayload(Map<String, ByteBuffer> payload) {
        this.outgoingPayload = payload == null ? null : ImmutableMap.copyOf(payload);
        return this;
    }

    protected static Boolean isBatchIdempotent(Collection<? extends Statement> statements) {
        boolean hasNullIdempotentStatements = false;
        for (Statement statement : statements) {
            Boolean innerIdempotent = statement.isIdempotent();
            if (innerIdempotent == null) {
                hasNullIdempotentStatements = true;
            } else if (!innerIdempotent) {
                return false;
            }
        }
        return (hasNullIdempotentStatements) ? null : true;
    }
}
