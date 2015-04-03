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
package com.datastax.driver.integration;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * A 'BaseIntegrationTest' that uses a single node CCM cluster and runs a test that exercises basic functionality.
 *
 * The intent of this is test is to be extended in other modules to exercise logic in different packaging
 * configurations that would not be covered by integration tests in driver-core.
 */
public abstract class BaseIntegrationTest extends CCMBridge.PerClassSingleNodeCluster{

    private static final Logger logger = LoggerFactory.getLogger(BaseIntegrationTest.class);

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.emptyList();
    }

    /**
     * <p>
     * Executes a basic integration test that does the following:
     *
     * <ol>
     *     <li>Creates a table.</li>
     *     <li>Prepares statements for inserting, reading and deleting data.</li>
     *     <li>Inserts 1000 rows into a table in 100 different partitions.</li>
     *     <li>Reads data from each partition.</li>
     *     <li>Reads all data with a range query in a paged fashion.</li>
     *     <li>Deletes the data.</li>
     *     <li>Deletes the table.</li>
     * </ol>
     */
    @Test(groups="short")
    public void should_perform_basic_functionality() throws Exception {
        String tableName = "spbf";
        basicLifecycle(session, tableName, 100, 10).call();
    }

    public Callable<Void> basicLifecycle(Session session, String tableName, int partitions, int keysPerPartition) {
        return new BasicLifecycle(session, tableName, partitions, keysPerPartition);
    }

    protected static class BasicLifecycle implements Callable<Void> {

        private final Session session;
        private final String tableName;
        private final int partitions;
        private final int rowsPerPartition;

        PreparedStatement updateStmt;
        PreparedStatement readStmt;
        PreparedStatement rangeStmt;
        PreparedStatement deleteStmt;

        protected BasicLifecycle(Session session, String tableName, int partitions, int rowsPerPartition) {
            this.session = session;
            this.tableName = tableName;
            this.partitions = partitions;
            this.rowsPerPartition = rowsPerPartition;
        }

        @Override
        public Void call() {
            createTable();
            prepare();
            insertData();
            readData();
            readDataRange();
            deleteData();
            dropData();
            return null;
        }

        public void createTable() {
            // TODO: Check if schema agrees / wait until it does.
            logger.debug("Creating {} table.", tableName);
            ResultSet tableResults = session.execute(
                    String.format("create table %s (k1 int, k2 int, v int, primary key (k1,k2))", tableName));
        }

        public void prepare() {
            updateStmt = session.prepare(QueryBuilder.update(tableName)
                    .with(set("v", bindMarker("v1")))
                    .where(eq("k1", bindMarker("k1"))).and(eq("k2", bindMarker("k2"))));

            readStmt = session.prepare(QueryBuilder.select().from(tableName)
                    .where(eq("k1", bindMarker("k1"))));

            rangeStmt = session.prepare(QueryBuilder.select().from(tableName));

            deleteStmt = session.prepare(QueryBuilder.delete().from(tableName)
                    .where(eq("k1", bindMarker("k1"))));
        }

        /**
         * Loads data into {@link #tableName} for {@link #partitions} with {@link #rowsPerPartition} rows per
         * partition.  Data from each partition is loaded as a {@link BatchStatement} and each statement is
         * executed concurrently using {@link Session#executeAsync}.  Will throw an exception if any insert fails.
         */
        public void insertData() {
            // Insert data.
            final CountDownLatch latch = new CountDownLatch(partitions);
            final AtomicReference<Throwable> failure = new AtomicReference<Throwable>(null);
            logger.debug("Performing {} batches with {} updates per batch.", partitions, rowsPerPartition);
            long startTime = System.currentTimeMillis();
            for(int k1 = 0; k1 < partitions; k1++) {
                BatchStatement batch = new BatchStatement();
                for(int k2 = 0; k2 < rowsPerPartition; k2++) {
                    batch.add(updateStmt.bind(k1+k2, k1, k2));
                }
                ResultSetFuture future = session.executeAsync(batch);
                Futures.addCallback(future, new FutureCallback<ResultSet>() {

                    @Override
                    public void onSuccess(ResultSet result) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        // Complete the latch on any failure and record the error.
                        if(failure.compareAndSet(null, t)) {
                            for (int i = 0; i < partitions; i++) {
                                latch.countDown();
                            }
                        }
                    }
                });
            }

            Uninterruptibles.awaitUninterruptibly(latch, 5, TimeUnit.SECONDS);
            Assertions.assertThat(latch.getCount()).isEqualTo(0);
            // Check to make sure no inserts failed.
            Throwable failureEx = failure.get();
            if(failureEx != null) {
                Assertions.fail("Error raised while inserting data.", failureEx);
            }
            logger.debug("Took {} ms to write data.", System.currentTimeMillis() - startTime);
        }

        /**
         * Reads data from {@link #tableName} for each of {@link #partitions}.  Each partition is queried individually
         * and all queries are made concurrently using {@link Session#executeAsync}.  Will throw an exception if any
         * read fails *or* the data returned is not expected.
         */
        public void readData() {
            // Read the data back.
            logger.debug("Performing {} reads.", partitions);
            final CountDownLatch readLatch = new CountDownLatch(partitions);
            final AtomicReference<Throwable> readFailure = new AtomicReference<Throwable>(null);
            long startTime = System.currentTimeMillis();
            for(int k1 = 0; k1 < partitions; k1++) {
                ResultSetFuture future = session.executeAsync(readStmt.bind(k1));
                Futures.addCallback(future, new FutureCallback<ResultSet>() {

                    private void setException(Throwable t) {
                        // Complete the latch on any failure and record the error.
                        if(readFailure.compareAndSet(null, t)) {
                            for (int i = 0; i < partitions; i++) {
                                readLatch.countDown();
                            }
                        }
                    }

                    @Override
                    public void onSuccess(ResultSet result) {
                        int expectedk2 = 0;
                        for(Row row : result) {
                            int k1 = row.getInt("k1");
                            int k2 = row.getInt("k2");
                            int v = row.getInt("v");

                            if(k2 != expectedk2) {
                                setException(new Exception(String.format("Expected k2 to be %d for %d:%d:%d", expectedk2, k1, k2, v)));
                                return;
                            }
                            if(v != k1+k2) {
                                setException(new Exception(String.format("Expected v to be %d for %d:%d:%d", k1+k2, k1, k2, v)));
                                return;
                            }
                            expectedk2++;
                        }
                        readLatch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        setException(t);
                    }
                });
            }

            Uninterruptibles.awaitUninterruptibly(readLatch, 5, TimeUnit.SECONDS);
            Assertions.assertThat(readLatch.getCount()).isEqualTo(0);
            // Check to make sure no inserts failed.
            Throwable failureEx = readFailure.get();
            if(failureEx != null) {
                Assertions.fail("Error raised while reading data.", failureEx);
            }
            logger.debug("Took {} ms to read data.", System.currentTimeMillis() - startTime);
        }

        /**
         * Reads data from {@link #tableName} as a range query.  Will throw an exception if the query fails *or* the
         * data returned is not expected.
         */
        public void readDataRange() {
            long startTime = System.currentTimeMillis();
            ResultSet rangeResults = session.execute(rangeStmt.bind());
            int count = 0;
            int expectedk2 = 0;
            int previousk1 = -1;
            logger.debug("Performing range query.");
            for(Row row : rangeResults) {
                int k1 = row.getInt("k1");
                if(k1 != previousk1) {
                    expectedk2 = 0;
                    previousk1 = k1;
                }
                int k2 = row.getInt("k2");
                Assertions.assertThat(k2).isEqualTo(expectedk2++);
                int v = row.getInt("v");
                Assertions.assertThat(v).isEqualTo(k1+k2);
                count++;
            }
            Assertions.assertThat(count).isEqualTo(partitions* rowsPerPartition);
            logger.debug("Took {} ms to read data from range query.", System.currentTimeMillis() - startTime);

        }

        /**
         * Deletes all data from {@link #tableName}.  Each partition is deleted individually and all deletes are done
         * asynchronously.  Will throw an exception if any delete fails.
         */
        public void deleteData() {
            // Delete the data.
            logger.debug("Performing {} deletes.", partitions);
            final CountDownLatch deleteLatch = new CountDownLatch(partitions);
            final AtomicReference<Throwable> deleteFailure = new AtomicReference<Throwable>(null);
            long startTime = System.currentTimeMillis();
            for(int k1 = 0; k1 < partitions; k1++) {
                ResultSetFuture future = session.executeAsync(deleteStmt.bind(k1));
                Futures.addCallback(future, new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet result) {
                        deleteLatch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        // Complete the latch on any failure and record the error.
                        if(deleteFailure.compareAndSet(null, t)) {
                            for (int i = 0; i < partitions; i++) {
                                deleteLatch.countDown();
                            }
                        }
                    }
                });
            }

            Uninterruptibles.awaitUninterruptibly(deleteLatch, 5, TimeUnit.SECONDS);
            Assertions.assertThat(deleteLatch.getCount()).isEqualTo(0);
            // Check to make sure no inserts failed.
            Throwable failureEx = deleteFailure.get();
            if(failureEx != null) {
                Assertions.fail("Error raised while deleting data.", failureEx);
            }
            logger.debug("Took {} ms to delete data.", System.currentTimeMillis() - startTime);
        }

        public void dropData() {
            logger.debug("Dropping {} table.", tableName);
            // TODO assert schema agreement.
            ResultSet dropResults = session.execute(String.format("drop table %s", tableName));
        }
    }
}
