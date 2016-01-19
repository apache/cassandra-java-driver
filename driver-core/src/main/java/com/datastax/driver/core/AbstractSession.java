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

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Abstract implementation of the Session interface.
 * <p/>
 * This is primarly intended to make mocking easier.
 */
public abstract class AbstractSession implements Session {

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(String query) {
        return execute(new SimpleStatement(query));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(String query, Object... values) {
        return execute(new SimpleStatement(query, values));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(String query, Map<String, Object> values) {
        return execute(new SimpleStatement(query, values));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(Statement statement) {
        return executeAsync(statement).getUninterruptibly();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsync(String query) {
        return executeAsync(new SimpleStatement(query));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsync(String query, Map<String, Object> values) {
        return executeAsync(new SimpleStatement(query, values));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsync(String query, Object... values) {
        return executeAsync(new SimpleStatement(query, values));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepare(String query) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareAsync(query));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepare(RegularStatement statement) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareAsync(statement));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        return prepareAsync(query, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(final RegularStatement statement) {

        if (statement.hasValues())
            throw new IllegalArgumentException("A statement to prepare should not have values");

        ListenableFuture<PreparedStatement> prepared = prepareAsync(statement.getQueryString(), statement.getOutgoingPayload());
        return Futures.transform(prepared, new Function<PreparedStatement, PreparedStatement>() {
            @Override
            public PreparedStatement apply(PreparedStatement prepared) {
                ProtocolVersion protocolVersion = getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
                CodecRegistry codecRegistry = getCluster().getConfiguration().getCodecRegistry();
                ByteBuffer routingKey = statement.getRoutingKey(protocolVersion, codecRegistry);
                if (routingKey != null)
                    prepared.setRoutingKey(routingKey);
                prepared.setConsistencyLevel(statement.getConsistencyLevel());
                if (statement.isTracing())
                    prepared.enableTracing();
                prepared.setRetryPolicy(statement.getRetryPolicy());
                prepared.setOutgoingPayload(statement.getOutgoingPayload());
                prepared.setIdempotent(statement.isIdempotent());

                return prepared;
            }
        });
    }

    /**
     * Prepares the provided query string asynchronously,
     * sending along the provided custom payload, if any.
     *
     * @param query         the CQL query string to prepare
     * @param customPayload the custom payload to send along the query, or {@code null} if no payload is to be sent
     * @return a future on the prepared statement corresponding to {@code query}.
     */
    protected abstract ListenableFuture<PreparedStatement> prepareAsync(String query, Map<String, ByteBuffer> customPayload);

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
