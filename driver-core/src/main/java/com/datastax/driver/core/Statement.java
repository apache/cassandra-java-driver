/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
 * This represents either a {@link RegularStatement}, a {@link BoundStatement} or a
 * {@link BatchStatement} along with the querying options (consistency level,
 * whether to trace the query, ...).
 */
public abstract class Statement {

    // An exception to the RegularStatement, BoundStatement or BatchStatement rule above. This is
    // used when preparing a statement and for other internal queries. Do not expose publicly.
    static final Statement DEFAULT = new Statement() {
        @Override
        public ByteBuffer getRoutingKey() { return null; }

        @Override
        public String getKeyspace() { return null; }
    };

    private volatile ConsistencyLevel consistency;
    private volatile ConsistencyLevel serialConsistency;
    private volatile boolean traceQuery;
    private volatile int fetchSize;
    private volatile long defaultTimestamp;

    private volatile RetryPolicy retryPolicy;

    // We don't want to expose the constructor, because the code relies on this being only sub-classed by RegularStatement, BoundStatement and BatchStatement
    Statement() {}

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
     * Sets the default timestamp for this query (in microseconds since the epoch).
     * <p>
     * This feature is only available when version {@link ProtocolVersion#V3 V3} or
     * higher of the native protocol is in use. With earlier versions, calling this
     * method has no effect.
     * <p>
     * The actual timestamp that will be used for this query is, in order of
     * preference:
     * <ul>
     * <li>the timestamp specified directly in the CQL query string (using the
     * {@code USING TIMESTAMP} syntax);</li>
     * <li>the timestamp specified through this method, if strictly positive;</li>
     * <li>the timestamp returned by the {@link TimestampGenerator} currently in use,
     * if strictly positive.</li>
     * </ul>
     * If none of these apply, no timestamp will be sent with the query and Cassandra
     * will generate a server-side one (similar to the pre-V3 behavior).
     *
     * @param defaultTimestamp the default timestamp for this query (must be strictly
     * positive).
     * @return this {@code Statement} object.
     *
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
}
