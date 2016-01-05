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
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.ProtocolVersion.V4;

public class DefaultPreparedStatement implements PreparedStatement {

    final PreparedId preparedId;

    final String query;
    final String queryKeyspace;
    final Map<String, ByteBuffer> incomingPayload;
    final Cluster cluster;

    volatile ByteBuffer routingKey;

    volatile ConsistencyLevel consistency;
    volatile ConsistencyLevel serialConsistency;
    volatile boolean traceQuery;
    volatile RetryPolicy retryPolicy;
    volatile ImmutableMap<String, ByteBuffer> outgoingPayload;
    volatile Boolean idempotent;

    private DefaultPreparedStatement(PreparedId id, String query, String queryKeyspace, Map<String, ByteBuffer> incomingPayload, Cluster cluster) {
        this.preparedId = id;
        this.query = query;
        this.queryKeyspace = queryKeyspace;
        this.incomingPayload = incomingPayload;
        this.cluster = cluster;
    }

    static DefaultPreparedStatement fromMessage(Responses.Result.Prepared msg, Cluster cluster, String query, String queryKeyspace) {
        assert msg.metadata.columns != null;

        ColumnDefinitions defs = msg.metadata.columns;

        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();

        if (defs.size() == 0) {
            return new DefaultPreparedStatement(new PreparedId(msg.statementId, defs, msg.resultMetadata.columns, null, protocolVersion), query, queryKeyspace, msg.getCustomPayload(), cluster);
        }

        int[] pkIndices = (protocolVersion.compareTo(V4) >= 0)
                ? msg.metadata.pkIndices
                : computePkIndices(cluster.getMetadata(), defs);

        PreparedId prepId = new PreparedId(msg.statementId, defs, msg.resultMetadata.columns, pkIndices, protocolVersion);

        return new DefaultPreparedStatement(prepId, query, queryKeyspace, msg.getCustomPayload(), cluster);
    }

    private static int[] computePkIndices(Metadata clusterMetadata, ColumnDefinitions boundColumns) {
        List<ColumnMetadata> partitionKeyColumns = null;
        int[] pkIndexes = null;
        KeyspaceMetadata km = clusterMetadata.getKeyspace(Metadata.quote(boundColumns.getKeyspace(0)));
        if (km != null) {
            TableMetadata tm = km.getTable(Metadata.quote(boundColumns.getTable(0)));
            if (tm != null) {
                partitionKeyColumns = tm.getPartitionKey();
                pkIndexes = new int[partitionKeyColumns.size()];
                for (int i = 0; i < pkIndexes.length; ++i)
                    pkIndexes[i] = -1;
            }
        }

        // Note: we rely on the fact CQL queries cannot span multiple tables. If that change, we'll have to get smarter.
        for (int i = 0; i < boundColumns.size(); i++)
            maybeGetIndex(boundColumns.getName(i), i, partitionKeyColumns, pkIndexes);

        return allSet(pkIndexes) ? pkIndexes : null;
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
        if (serialConsistency != ConsistencyLevel.SERIAL && serialConsistency != ConsistencyLevel.LOCAL_SERIAL)
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
    public Map<String, ByteBuffer> getIncomingPayload() {
        return incomingPayload;
    }

    @Override
    public Map<String, ByteBuffer> getOutgoingPayload() {
        return outgoingPayload;
    }

    @Override
    public PreparedStatement setOutgoingPayload(Map<String, ByteBuffer> payload) {
        this.outgoingPayload = payload == null ? null : ImmutableMap.copyOf(payload);
        return this;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return cluster.getConfiguration().getCodecRegistry();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement setIdempotent(Boolean idempotent) {
        this.idempotent = idempotent;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean isIdempotent() {
        return this.idempotent;
    }
}
