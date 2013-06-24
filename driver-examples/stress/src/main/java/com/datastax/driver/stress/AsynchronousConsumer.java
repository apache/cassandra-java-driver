/*
 *      Copyright (C) 2012 DataStax Inc.
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
package com.datastax.driver.stress;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.TimerContext;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;

public class AsynchronousConsumer implements Consumer {

    private static final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final Session session;
    private final Iterator<QueryGenerator.Request> requests;
    private final Reporter reporter;

    public AsynchronousConsumer(Session session,
                                Iterator<QueryGenerator.Request> requests,
                                Reporter reporter) {
        this.session = session;
        this.requests = requests;
        this.reporter = reporter;
    }

    @Override
    public void start() {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                request();
            }
        });
    }

    private void request() {

        if (!requests.hasNext()) {
            shutdown();
            return;
        }

        handle(requests.next());
    }

    @Override
    public void join() {
        awaitUninterruptibly(shutdownLatch);
    }

    protected void handle(QueryGenerator.Request request) {

        final TimerContext timerContext = reporter.latencies.time();
        ResultSetFuture resultSetFuture = request.executeAsync(session);
        Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(final ResultSet result) {
                timerContext.stop();
                reporter.requests.mark();
                request();
            }

            @Override
            public void onFailure(final Throwable t) {
                // Could do better I suppose
                System.err.println("Error during request: " + t);
                timerContext.stop();
                reporter.requests.mark();
                request();
            }
        }, executorService);
    }

    protected void shutdown() {
        shutdownLatch.countDown();
    }
}
