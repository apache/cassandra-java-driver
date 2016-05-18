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
import java.util.Map;

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
        PreparedId id = PreparedId.fromMessage(msg, cluster);
        return new DefaultPreparedStatement(id, query, queryKeyspace, msg.getCustomPayload(), cluster);
    }

    @Override
    public ColumnDefinitions getVariables() {
        return preparedId.getVariables();
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
