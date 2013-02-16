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

import java.util.concurrent.*;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.TimerContext;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;

public class Consumer extends Thread {

    protected final Session session;
    protected final BlockingQueue<QueryGenerator.Request> workQueue;
    protected final Reporter reporter;

    public Consumer(Session session, BlockingQueue<QueryGenerator.Request> workQueue, Reporter reporter) {
        super("Consumer Thread");
        this.session = session;
        this.workQueue = workQueue;
        this.reporter = reporter;
        this.setDaemon(true);
    }

    public void run() {
        try {

            while (true) {
                QueryGenerator.Request request = workQueue.take();
                if (request == QueryGenerator.DONE_MARKER) {
                    shutdown();
                    return;
                }

                handle(request);
            }

        } catch (InterruptedException e) {
            System.err.println("Consumer interrupted" + (e.getMessage() != null ? ": " + e.getMessage() : ""));
        } catch (DriverException e) {
            System.err.println("Error during query: " + e.getMessage());
        }
    }

    protected void shutdown() {}

    protected void handle(QueryGenerator.Request request) {
        TimerContext context = reporter.latencies.time();
        try {
            request.execute(session);
        } finally {
            context.stop();
        }
        reporter.requests.mark();
    }

    public static class Asynchronous extends Consumer {

        private final BlockingQueue<Asynchronous.Result> resultQueue;

        public Asynchronous(Session session, BlockingQueue<QueryGenerator.Request> workQueue, Reporter reporter, ResultHandler resultHandler) {
            super(session, workQueue, reporter);
            this.resultQueue = resultHandler.queue;
        }

        protected void handle(QueryGenerator.Request request) {
            TimerContext context = reporter.latencies.time();
            resultQueue.offer(new Result(request.executeAsync(session), context, reporter.requests));
        }

        protected void shutdown() {
            resultQueue.offer(Result.END_MARKER);
        }

        private static class Result {

            static final Result END_MARKER = new Result(null, null, null);

            public final ResultSetFuture future;
            public final TimerContext context;
            public final Meter requests;

            public Result(ResultSetFuture future, TimerContext context, Meter requests) {
                this.future = future;
                this.context = context;
                this.requests = requests;
            }
        }

        public static class ResultHandler extends Thread {

            private final BlockingQueue<Asynchronous.Result> queue = new LinkedBlockingQueue<Asynchronous.Result>();

            public ResultHandler() {
                super("Result Eater Thread");
                this.setDaemon(true);
            }

            public void run() {
                try {

                    while (true) {
                        Result res = queue.take();
                        if (res == Result.END_MARKER)
                            return;

                        try {
                            res.future.getUninterruptibly();
                        } finally {
                            res.context.stop();
                        }
                        res.requests.mark();
                    }

                } catch (InterruptedException e) {
                    System.err.println("Consumer interrupted" + (e.getMessage() != null ? ": " + e.getMessage() : ""));
                } catch (DriverException e) {
                    System.err.println("Error retrieving result to query: " + e.getMessage());
                }
            }
        }
    }
}
