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

import com.datastax.driver.core.policies.RetryPolicy;

import java.nio.ByteBuffer;
import java.util.List;

public class DefaultPreparedStatement implements IdempotenceAwarePreparedStatement {

    final PreparedId preparedId;

    final String query;
    final String queryKeyspace;

    volatile ByteBuffer routingKey;

    volatile ConsistencyLevel consistency;
    volatile ConsistencyLevel serialConsistency;
    volatile boolean traceQuery;
    volatile RetryPolicy retryPolicy;
    volatile Boolean idempotent;

    private DefaultPreparedStatement(PreparedId id, String query, String queryKeyspace) {
        this.preparedId = id;
        this.query = query;
        this.queryKeyspace = queryKeyspace;
    }

    static DefaultPreparedStatement fromMessage(Responses.Result.Prepared msg, Metadata clusterMetadata, ProtocolVersion protocolVersion, String query, String queryKeyspace) {
        assert msg.metadata.columns != null;

        ColumnDefinitions defs = msg.metadata.columns;

        if (defs.size() == 0)
            return new DefaultPreparedStatement(new PreparedId(msg.statementId, defs, msg.resultMetadata.columns, null, protocolVersion), query, queryKeyspace);

        List<ColumnMetadata> partitionKeyColumns = null;
        int[] pkIndexes = null;
        KeyspaceMetadata km = clusterMetadata.getKeyspace(Metadata.quote(defs.getKeyspace(0)));
        if (km != null) {
            TableMetadata tm = km.getTable(Metadata.quote(defs.getTable(0)));
            if (tm != null) {
                partitionKeyColumns = tm.getPartitionKey();
                pkIndexes = new int[partitionKeyColumns.size()];
                for (int i = 0; i < pkIndexes.length; ++i)
                    pkIndexes[i] = -1;
            }
        }

        // Note: we rely on the fact CQL queries cannot span multiple tables. If that change, we'll have to get smarter.
        for (int i = 0; i < defs.size(); i++)
            maybeGetIndex(defs.getName(i), i, partitionKeyColumns, pkIndexes);

        PreparedId prepId = new PreparedId(msg.statementId, defs, msg.resultMetadata.columns, allSet(pkIndexes) ? pkIndexes : null, protocolVersion);

        return new DefaultPreparedStatement(prepId, query, queryKeyspace);
    }

    private static void maybeGetIndex(String name, int j, List<ColumnMetadata> pkColumns, int[] pkIndexes) {
        if (pkColumns == null)
            return;

        for (int i = 0; i < pkColumns.size(); ++i) {
            if (name.equals(pkColumns.get(i).getName())) {
                // We may have the same column prepared multiple times, but only pick the first value
                pkIndexes[i] = j;
                return;
            }
        }
    }

    private static boolean allSet(int[] pkColumns) {
        if (pkColumns == null)
            return false;

        for (int i = 0; i < pkColumns.length; ++i)
            if (pkColumns[i] < 0)
                return false;

        return true;
    }

    @Override
    public ColumnDefinitions getVariables() {
        return preparedId.metadata;
    }

    @Override
    public BoundStatement bind(Object... values) {
        BoundStatement bs = new BoundStatement(this);
        return bs.bind(values);
    }

    @Override
    public BoundStatement bind() {
        return new BoundStatement(this);
    }

    @Override
    public PreparedStatement setRoutingKey(ByteBuffer routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    @Override
    public PreparedStatement setRoutingKey(ByteBuffer... routingKeyComponents) {
        this.routingKey = SimpleStatement.compose(routingKeyComponents);
        return this;
    }

    @Override
    public ByteBuffer getRoutingKey() {
        return routingKey;
    }

    @Override
    public PreparedStatement setConsistencyLevel(ConsistencyLevel consistency) {
        this.consistency = consistency;
        return this;
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }

    @Override
    public PreparedStatement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
        if (!serialConsistency.isSerial())
            throw new IllegalArgumentException();
        this.serialConsistency = serialConsistency;
        return this;
    }

    @Override
    public ConsistencyLevel getSerialConsistencyLevel() {
        return serialConsistency;
    }

    @Override
    public String getQueryString() {
        return query;
    }

    @Override
    public String getQueryKeyspace() {
        return queryKeyspace;
    }

    @Override
    public PreparedStatement enableTracing() {
        this.traceQuery = true;
        return this;
    }

    @Override
    public PreparedStatement disableTracing() {
        this.traceQuery = false;
        return this;
    }

    @Override
    public boolean isTracing() {
        return traceQuery;
    }

    @Override
    public PreparedStatement setRetryPolicy(RetryPolicy policy) {
        this.retryPolicy = policy;
        return this;
    }

    @Override
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    @Override
    public PreparedId getPreparedId() {
        return preparedId;
    }

    @Override
    public PreparedStatement setIdempotent(Boolean idempotent) {
        this.idempotent = idempotent;
        return this;
    }

    @Override
    public Boolean isIdempotent() {
        return this.idempotent;
    }
}
