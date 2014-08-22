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
import java.util.concurrent.ExecutionException;

import com.google.common.base.Function;
import com.google.common.util.concurrent.*;

/**
 * Abstract implementation of the Session interface.
 *
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
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
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
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(final RegularStatement statement) {
        if (statement.getValues() != null)
            throw new IllegalArgumentException("A statement to prepare should not have values");

        ListenableFuture<PreparedStatement> prepared = prepareAsync(statement.toString());
        return Futures.transform(prepared, new Function<PreparedStatement, PreparedStatement>() {
            @Override
            public PreparedStatement apply(PreparedStatement prepared) {
                ByteBuffer routingKey = statement.getRoutingKey();
                if (routingKey != null)
                    prepared.setRoutingKey(routingKey);
                prepared.setConsistencyLevel(statement.getConsistencyLevel());
                if (statement.isTracing())
                    prepared.enableTracing();
                prepared.setRetryPolicy(statement.getRetryPolicy());

                return prepared;
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
