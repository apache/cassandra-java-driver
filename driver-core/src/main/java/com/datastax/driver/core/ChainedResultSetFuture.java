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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@code ResultSetFuture} that will complete when its source future completes.
 */
class ChainedResultSetFuture extends AbstractFuture<ResultSet> implements ResultSetFuture {

    private volatile ResultSetFuture source;

    void setSource(ResultSetFuture source) {
        if (this.isCancelled())
            source.cancel(false);
        this.source = source;
        Futures.addCallback(source, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                ChainedResultSetFuture.this.set(result);
            }

            @Override
            public void onFailure(Throwable t) {
                ChainedResultSetFuture.this.setException(t);
            }
        });
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return (source == null || source.cancel(mayInterruptIfRunning))
                && super.cancel(mayInterruptIfRunning);
    }

    @Override
    public ResultSet getUninterruptibly() {
        try {
            return Uninterruptibles.getUninterruptibly(this);
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    @Override
    public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            return Uninterruptibles.getUninterruptibly(this, timeout, unit);
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }
}

