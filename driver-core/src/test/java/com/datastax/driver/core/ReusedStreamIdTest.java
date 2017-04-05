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

import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.fail;

@CreateCCM(PER_METHOD)
public class ReusedStreamIdTest extends CCMTestsSupport {

    static Logger logger = LoggerFactory.getLogger(ReusedStreamIdTest.class);

    @SuppressWarnings("unused")
    public Cluster.Builder createClusterLowReadTimeout() {
        // Low Read Timeout to trigger retry behavior.
        return Cluster.builder().withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000));
    }

    /**
     * Ensures that if activity tied to future completion blocks netty io threads that this does not cause the
     * driver to possibly invoke the wrong handler for a response, as described in JAVA-1179.
     * <p/>
     * This is accomplished by setting up a 2 node cluster and setting a low read timeout (1 second).  The test
     * submits 10 concurrent requests repeatedly for up to 500 queries and on completion of each request may block
     * in a callback randomly (25% of the time) for 1 second, causing a retry on the next host to trigger.  If a new
     * stream id is not allocated on the retry, its possible it could use an already used stream id and cause the driver
     * to invoke the wrong handlers for a response.   The test checks for this by ensuring that the column returned in a
     * response matches the one queried.  If the column received does not match, the test fails.  In cases where this
     * bug is present, it should be detected within 10 seconds.
     *
     * @jira_ticket JAVA-1179
     * @test_category queries:async_callback
     */
    @Test(groups = "long")
    @CCMConfig(numberOfNodes = 2, clusterProvider = "createClusterLowReadTimeout")
    public void should_not_receive_wrong_response_when_callbacks_block_io_thread() {
        int concurrency = 10;
        final Semaphore semaphore = new Semaphore(concurrency);
        // RNG to determine sleep times.
        final Random random = new Random();

        try {
            // Use the system.local table and alternate between columns that are queried.
            List<ColumnMetadata> columnsToGrab = cluster().getMetadata().getKeyspace("system").getTable("local").getColumns();
            assertThat(columnsToGrab.size()).isGreaterThan(1);

            final CountDownLatch errorTrigger = new CountDownLatch(1);

            long start = System.currentTimeMillis();
            // 500 iterations will take roughly 1 minute.
            int iterations = 500;
            final AtomicInteger completed = new AtomicInteger(0);

            for (int i = 1; i <= iterations; i++) {
                try {
                    if (errorTrigger.getCount() == 0) {
                        fail(String.format("Error triggered at or before %d of %d requests after %dms.", i, iterations,
                                System.currentTimeMillis() - start));
                    }
                    semaphore.acquire();
                    final String column = columnsToGrab.get(i % columnsToGrab.size()).getName();
                    String query = String.format("select %s from system.local", column);
                    ResultSetFuture future = session().executeAsync(query);

                    Futures.addCallback(future, new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(ResultSet result) {
                            semaphore.release();
                            // Expect the column that you queried to be present, if its not we got the wrong response
                            // back.
                            int columnIndex = result.getColumnDefinitions().getIndexOf(column);
                            if (columnIndex == -1) {
                                logger.error("Got response without column {}, got columns {} from Host {}.", column,
                                        result.getColumnDefinitions(), result.getExecutionInfo().getQueriedHost());
                                errorTrigger.countDown();
                                return;
                            }
                            completed.incrementAndGet();
                            // Block netty io thread 25% of the time.
                            int num = random.nextInt(1);
                            if (num == 0) {
                                // Sleep exactly one second, should trigger retry.
                                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            semaphore.release();
                            // Timeouts are inevitable because of low query timeouts and blocked threads.
                            if (!(t instanceof OperationTimedOutException)) {
                                logger.error("Unexpected error encountered.", t);
                                errorTrigger.countDown();
                            }
                        }
                    });
                } catch (InterruptedException e) {
                    fail("Test interrupted", e);
                }
                if (i % (iterations / 10) == 0) {
                    logger.info("Submitted {} of {} requests. ({} completed successfully)", i, iterations, completed.get());
                }
            }

            // Wait for 10 seconds for any remaining requests to possibly trigger an error, its likely
            // that if we get to this point this will not happen.
            Uninterruptibles.awaitUninterruptibly(errorTrigger, 10, TimeUnit.SECONDS);
            if (errorTrigger.getCount() == 0) {
                fail(String.format("Error triggered after %dms.", System.currentTimeMillis() - start));
            }
            // Sanity check to ensure that at least some requests succeeded, we expect some failures if both
            // hosts timeout as its likely they could be blocked on the event loop.
            assertThat(completed.get()).isGreaterThan(0);
        } finally {
            try {
                // Acquire all permits to make sure all inflight requests complete.
                if (!semaphore.tryAcquire(concurrency, 10, TimeUnit.SECONDS)) {
                    fail("Could not acquire all permits within 10 seconds of completion.");
                }
            } catch (InterruptedException e) {
                fail("Interrupted.", e);
            }
        }
    }
}
