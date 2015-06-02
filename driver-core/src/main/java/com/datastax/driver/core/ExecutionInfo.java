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
import java.util.List;

import com.datastax.driver.core.utils.Bytes;

/**
 * Basic information on the execution of a query.
 */
public class ExecutionInfo {
    private final List<Host> triedHosts;
    private final ConsistencyLevel achievedConsistency;
    private final QueryTrace trace;
    private final ByteBuffer pagingState;
    private final ProtocolVersion protocolVersion;
    private final Statement statement;
    private volatile boolean schemaInAgreement;

    private ExecutionInfo(List<Host> triedHosts, ConsistencyLevel achievedConsistency, QueryTrace trace, ByteBuffer pagingState, ProtocolVersion protocolVersion, Statement statement, boolean schemaAgreement) {
        this.triedHosts = triedHosts;
        this.achievedConsistency = achievedConsistency;
        this.trace = trace;
        this.pagingState = pagingState;
        this.protocolVersion = protocolVersion;
        this.statement = statement;
        this.schemaInAgreement = schemaAgreement;
    }

    ExecutionInfo(List<Host> triedHosts) {
        this(triedHosts, null, null, null, null, null, true);
    }

    ExecutionInfo withTrace(QueryTrace newTrace) {
        return new ExecutionInfo(triedHosts, achievedConsistency, newTrace, pagingState, protocolVersion, statement, schemaInAgreement);
    }

    ExecutionInfo withAchievedConsistency(ConsistencyLevel newConsistency) {
        return new ExecutionInfo(triedHosts, newConsistency, trace, pagingState, protocolVersion, statement, schemaInAgreement);
    }

    ExecutionInfo withPagingState(ByteBuffer pagingState, ProtocolVersion protocolVersion) {
        return new ExecutionInfo(triedHosts, achievedConsistency, trace, pagingState, protocolVersion, statement, schemaInAgreement);
    }

    ExecutionInfo withStatement(Statement statement) {
        return new ExecutionInfo(triedHosts, achievedConsistency, trace, pagingState, protocolVersion, statement, schemaInAgreement);
    }

    /**
     * The list of tried hosts for this query.
     * <p>
     * In general, this will be a singleton list with the host that coordinated
     * that query. However:
     * <ul>
     * <li>if a host is tried by the driver but is dead or in
     * error, that host is recorded and the query is retried;</li>
     * <li>on a timeout or unavailable exception, some
     * {@link com.datastax.driver.core.policies.RetryPolicy} may retry the
     * query on the same host, so the same host might appear twice.</li>
     * <li>if {@link com.datastax.driver.core.policies.SpeculativeExecutionPolicy speculative executions}
     * are enabled, other hosts might have been tried speculatively as well.</li>
     * </ul>
     * <p>
     * If you are only interested in fetching the final (and often only) node
     * coordinating the query, {@link #getQueriedHost} provides a shortcut to
     * fetch the last element of the list returned by this method.
     *
     * @return the list of tried hosts for this query, in the order tried.
     */
    public List<Host> getTriedHosts() {
        return triedHosts;
    }

    /**
     * Return the Cassandra host that coordinated this query.
     * <p>
     * This is a shortcut for {@code getTriedHosts().get(getTriedHosts().size())}.
     *
     * @return return the Cassandra host that coordinated this query.
     */
    public Host getQueriedHost() {
        return triedHosts.get(triedHosts.size() - 1);
    }

    /**
     * If the query returned without achieving the requested consistency level
     * due to the {@link com.datastax.driver.core.policies.RetryPolicy}, this
     * return the biggest consistency level that has been actually achieved by
     * the query.
     * <p>
     * Note that the default {@code RetryPolicy}
     * ({@link com.datastax.driver.core.policies.DefaultRetryPolicy})
     * will never allow a query to be successful without achieving the
     * initially requested consistency level and hence with that default
     * policy, this method will <b>always</b> return {@code null}. However, it
     * might occasionally return a non-{@code null} with say,
     * {@link com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy}.
     *
     * @return {@code null} if the original consistency level of the query was
     * achieved, or the consistency level that was ultimately achieved if the
     * {@code RetryPolicy} triggered a retry at a different consistency level
     * than the original one.
     */
    public ConsistencyLevel getAchievedConsistencyLevel() {
        return achievedConsistency;
    }

    /**
     * The query trace if tracing was enabled on this query.
     *
     * @return the {@code QueryTrace} object for this query if tracing was
     * enable for this query, or {@code null} otherwise.
     */
    public QueryTrace getQueryTrace() {
        return trace;
    }

    /**
     * The paging state of the query.
     *
     * This object represents the next page to be fetched if this query is
     * multi page. It can be saved and reused later on the same statement.
     *
     * @return the paging state or null if there is no next page.
     *
     * @see Statement#setPagingState(PagingState)
     */
    public PagingState getPagingState() {
        if (this.pagingState == null)
            return null;
        return new PagingState(this.pagingState, this.statement, this.protocolVersion);
    }

    /**
     * Returns the "raw" paging state of the query.
     *
     * Contrary to {@link #getPagingState()}, there will be no validation when
     * this is later reinjected into a statement.
     *
     * @return the paging state or null if there is no next page.
     *
     * @see Statement#setPagingStateUnsafe(byte[])
     */
    public byte[] getPagingStateUnsafe() {
        if (this.pagingState == null)
            return null;
        return Bytes.getArray(this.pagingState);
    }

    /**
     * Whether the cluster had reached schema agreement after the execution of this query.
     *
     * After a successful schema-altering query (ex: creating a table), the driver
     * will check if the cluster's nodes agree on the new schema version. If not,
     * it will keep retrying for a given delay (configurable via
     * {@link Cluster.Builder#withMaxSchemaAgreementWaitSeconds(int)}).
     * <p>
     * If this method returns {@code false}, clients can call {@link Metadata#checkSchemaAgreement()}
     * later to perform the check manually.
     * <p>
     * Note that the schema agreement check is only performed for schema-altering queries
     * For other query types, this method will always return {@code true}.
     *
     * @return whether the cluster reached schema agreement, or {@code true} for a non
     * schema-altering statement.
     */
    public boolean isSchemaInAgreement() {
        return schemaInAgreement;
    }

    void setSchemaInAgreement(boolean schemaAgreement) {
        this.schemaInAgreement = schemaAgreement;
    }
}
