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

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.CassandraVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;

/**
 * Simple test of the Sessions methods against a one node cluster.
 */
public class SessionTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final Logger logger = LoggerFactory.getLogger(SessionTest.class);

    private static final String TABLE1 = "test1";
    private static final String TABLE2 = "test2";
    private static final String TABLE3 = "test3";
    private static final String COUNTER_TABLE = "counters";

    @Override
    protected Collection<String> getTableDefinitions() {
        return Arrays.asList(String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", TABLE1),
                             String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", TABLE2),
                             String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", TABLE3),
                             String.format("CREATE TABLE %s (k text PRIMARY KEY, c counter)", COUNTER_TABLE));
    }

    @Test(groups = "short")
    public void executeTest() throws Exception {
        // Simple calls to all versions of the execute/executeAsync methods
        String key = "execute_test";
        ResultSet rs = session.execute(String.format(Locale.US, "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)", TABLE1, key, "foo", 42, 24.03f));
        assertTrue(rs.isExhausted());

        // execute
        checkExecuteResultSet(session.execute(String.format(TestUtils.SELECT_ALL_FORMAT, TABLE1)), key);
        checkExecuteResultSet(session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, TABLE1)).setConsistencyLevel(ConsistencyLevel.ONE)), key);

        // executeAsync
        checkExecuteResultSet(session.executeAsync(String.format(TestUtils.SELECT_ALL_FORMAT, TABLE1)).getUninterruptibly(), key);
        checkExecuteResultSet(session.executeAsync(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, TABLE1)).setConsistencyLevel(ConsistencyLevel.ONE)).getUninterruptibly(), key);
    }

    @Test(groups = "short")
    public void executePreparedTest() throws Exception {
        // Simple calls to all versions of the execute/executeAsync methods for prepared statements
        // Note: the goal is only to exercice the Session methods, PreparedStatementTest have better prepared statement tests.
        String key = "execute_prepared_test";
        ResultSet rs = session.execute(String.format(Locale.US, "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)", TABLE2, key, "foo", 42, 24.03f));
        assertTrue(rs.isExhausted());

        PreparedStatement p = session.prepare(String.format(TestUtils.SELECT_ALL_FORMAT + " WHERE k = ?", TABLE2));
        BoundStatement bs = p.bind(key);

        // executePrepared
        checkExecuteResultSet(session.execute(bs), key);
        checkExecuteResultSet(session.execute(bs.setConsistencyLevel(ConsistencyLevel.ONE)), key);

        // executePreparedAsync
        checkExecuteResultSet(session.executeAsync(bs).getUninterruptibly(), key);
        checkExecuteResultSet(session.executeAsync(bs.setConsistencyLevel(ConsistencyLevel.ONE)).getUninterruptibly(), key);
    }

    private static void checkExecuteResultSet(ResultSet rs, String key) {
        assertTrue(!rs.isExhausted());
        Row row = rs.one();
        assertTrue(rs.isExhausted());
        assertEquals(row.getString("k"), key);
        assertEquals(row.getString("t"), "foo");
        assertEquals(row.getInt("i"), 42);
        assertEquals(row.getFloat("f"), 24.03f, 0.1f);
    }

    @Test(groups = "short")
    public void executePreparedCounterTest() throws Exception {
        PreparedStatement p = session.prepare("UPDATE " + COUNTER_TABLE + " SET c = c + ? WHERE k = ?");

        session.execute(p.bind(1L, "row"));
        session.execute(p.bind(1L, "row"));

        ResultSet rs = session.execute("SELECT * FROM " + COUNTER_TABLE);
        List<Row> rows = rs.all();
        assertEquals(rows.size(), 1);
        assertEquals(rows.get(0).getLong("c"), 2L);
    }

    /**
     * Validates that a session can be established using snappy compression and executes some queries that inserts and
     * retrieves data using that session.
     *
     * @test_category connection:compression
     * @expected_result session established and queries made successfully using it.
     */
    @Test(groups = "short")
    public void session_should_function_with_snappy_compression() throws Exception {
        compressionTest(ProtocolOptions.Compression.SNAPPY);
    }

    /**
     * Validates that a session can be established using lz4 compression and executes some queries that inserts and
     * retrieves data using that session.
     *
     * @test_category connection:compression
     * @expected_result session established and queries made successfully using it.
     */
    @Test(groups = "short")
    @CassandraVersion(major=2.0)
    public void session_should_function_with_lz4_compression() throws Exception {
        compressionTest(ProtocolOptions.Compression.LZ4);
    }

    public void compressionTest(ProtocolOptions.Compression compression) {
        cluster.getConfiguration().getProtocolOptions().setCompression(compression);
        try {
            Session compressedSession = cluster.connect(keyspace);

            // Simple calls to all versions of the execute/executeAsync methods
            String key = "execute_compressed_test_" + compression;
            ResultSet rs = compressedSession.execute(String.format(Locale.US, "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)", TABLE3, key, "foo", 42, 24.03f));
            assertTrue(rs.isExhausted());

            String SELECT_ALL = String.format(TestUtils.SELECT_ALL_FORMAT + " WHERE k = '%s'", TABLE3, key);

            // execute
            checkExecuteResultSet(compressedSession.execute(SELECT_ALL), key);
            checkExecuteResultSet(compressedSession.execute(new SimpleStatement(SELECT_ALL).setConsistencyLevel(ConsistencyLevel.ONE)), key);

            // executeAsync
            checkExecuteResultSet(compressedSession.executeAsync(SELECT_ALL).getUninterruptibly(), key);
            checkExecuteResultSet(compressedSession.executeAsync(new SimpleStatement(SELECT_ALL).setConsistencyLevel(ConsistencyLevel.ONE)).getUninterruptibly(), key);

        } finally {
            cluster.getConfiguration().getProtocolOptions().setCompression(ProtocolOptions.Compression.NONE);
        }
    }

    /**
     * Checks for deadlocks when a session shutdown races with the initialization of the cluster (JAVA-418).
     */
    @Test(groups = "short")
    public void closeDuringClusterInitTest() throws InterruptedException {
        for (int i = 0; i < 500; i++) {

            // Use our own cluster and session (not the ones provided by the parent class) because we want an uninitialized cluster
            // (note the use of newSession below)
            final Cluster cluster = Cluster.builder()
                .addContactPointsWithPorts(Collections.singletonList(hostAddress))
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();
            final Session session = cluster.newSession();

            // Spawn two threads to simulate the race
            ExecutorService executor = Executors.newFixedThreadPool(2);
            final CountDownLatch startLatch = new CountDownLatch(1);

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        startLatch.await();
                        cluster.init();
                    } catch (InterruptedException e) {
                        fail("unexpected interruption", e);
                    }
                }
            });

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        startLatch.await();
                        TimeUnit.MILLISECONDS.sleep(10);
                        session.close();
                    } catch (InterruptedException e) {
                        fail("unexpected interruption", e);
                    }
                }
            });

            // Start the threads
            startLatch.countDown();

            executor.shutdown();
            boolean normalShutdown = executor.awaitTermination(5, TimeUnit.SECONDS);
            assertTrue(normalShutdown);

            // The deadlock occurred here before JAVA-418
            cluster.close();
        }
    }
}
