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

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.utils.SocketChannelMonitor;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeoutStressTest {

    static final Logger logger = LoggerFactory.getLogger(TimeoutStressTest.class);

    // Maximum number of concurrent queries running at a given time.
    static final int CONCURRENT_QUERIES = 25;

    // How long the test should run for, may want to consider running for longer periods to time to check for leaks
    // that could occur over very tiny timing windows.
    static final long DURATION = 60000;

    // Configured read timeout - this may need to be tuned to the host system running the test.
    static final int READ_TIMEOUT_IN_MS = 50;

    // Configured connection timeout - this may need to be tuned to the host system running the test.
    static final int CONNECTION_TIMEOUT_IN_MS = 20;

    // Keyspace to use.
    static final String KEYSPACE = "testks";

    static Cluster cluster;

    static CCMBridge ccmBridge;

    static SocketChannelMonitor channelMonitor;

    static AtomicInteger executedQueries = new AtomicInteger(0);

    static List<InetSocketAddress> nodes;

    @BeforeClass(groups = "long")
    public void beforeClass() throws Exception {
        ccmBridge = CCMBridge.create("test", 3);
        channelMonitor = new SocketChannelMonitor();
        nodes = Lists.newArrayList(
                new InetSocketAddress(CCMBridge.IP_PREFIX + '1', 9042),
                new InetSocketAddress(CCMBridge.IP_PREFIX + '2', 9042),
                new InetSocketAddress(CCMBridge.IP_PREFIX + '3', 9042)
        );

        PoolingOptions poolingOptions = new PoolingOptions().setCoreConnectionsPerHost(HostDistance.LOCAL, 8);

        cluster = Cluster.builder()
                .addContactPointsWithPorts(nodes)
                .withPoolingOptions(poolingOptions)
                .withNettyOptions(channelMonitor.nettyOptions())
                .build();
    }

    @AfterClass(groups = "long")
    public void afterClass() {
        if(ccmBridge != null) {
            ccmBridge.stop();
        }
        if(channelMonitor != null) {
            channelMonitor.stop();
        }
        if(cluster != null) {
            cluster.close();
        }
    }

    /**
     * <p>
     * Validates that under extreme timeout conditions the driver is able to properly maintain connection pools in
     * addition to not leaking connections.
     *
     * <p>
     * Does the following:
     * <ol>
     *     <li>Creates a table and loads 30k rows in a single partition.</li>
     *     <li>Sets the connection and read timeout {@link SocketOptions} to very low values.</li>
     *     <li>Spawns workers that concurrently execute queries.</li>
     *     <li>For some duration, repeatedly measures number of open socket connections and warns if exceeded.</li>
     *     <li>After a duration, resets {@link SocketOptions} to defaults.</li>
     *     <li>Wait for 20 seconds for reaper to remove old connections and restore pools.</li>
     *     <li>Ensure pools are restored.</li>
     *     <li>Shutdown session and ensure that there remains only 1 open connection.</li>
     * </ol>
     *
     * @test_category connection:connection_pool
     * @expected_result no connections leak and all host pools are maintained.
     * @jira_ticket JAVA-692
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "long")
    public void host_state_should_be_maintained_with_timeouts() throws Exception {
        // Setup schema and insert records.
        Session session = cluster.connect();
        setupSchema(session);
        session.close();

        // Set very low timeouts.
        cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(CONNECTION_TIMEOUT_IN_MS);
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(READ_TIMEOUT_IN_MS);
        session = cluster.connect();
        PreparedStatement statement = session.prepare("select * from " + KEYSPACE + ".record where name=? limit 1000;");

        int workers = Runtime.getRuntime().availableProcessors();
        ExecutorService workerPool = Executors.newFixedThreadPool(workers,
                new ThreadFactoryBuilder().setNameFormat("timeout-stress-test-worker-%d").setDaemon(true).build());

        AtomicBoolean stopped = new AtomicBoolean(false);

        // Ensure that we never exceed MaxConnectionsPerHost * nodes + 1 control connection.
        int maxConnections = TestUtils.numberOfLocalCoreConnections(cluster) * nodes.size() + 1;

        try {
            Semaphore concurrentQueries = new Semaphore(CONCURRENT_QUERIES);
            for (int i = 0; i < workers; i++) {
                workerPool.submit(new TimeoutStressWorker(session, statement, concurrentQueries, stopped));
            }

            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < DURATION) {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                channelMonitor.report();
                // Some connections that are being closed may have had active requests which are delegated to the
                // reaper for cleanup later.
                Collection<SocketChannel> openChannels = channelMonitor.openChannels(nodes);

                // Ensure that we don't exceed maximum connections.  Log as warning as there will be a bit of a timing
                // factor between retrieving open connections and checking the reaper.
                if(openChannels.size() > maxConnections) {
                    logger.warn("{} of open channels: {} exceeds maximum expected: {}.  " +
                                    "This could be because there are connections to be cleaned up in the reaper.",
                            openChannels.size(), maxConnections, openChannels);
                }
            }
        } finally {
            stopped.set(true);

            // Reset socket timeouts to allow pool to recover.
            cluster.getConfiguration().getSocketOptions()
                    .setConnectTimeoutMillis(SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS);
            cluster.getConfiguration().getSocketOptions()
                    .setReadTimeoutMillis(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS);

            logger.debug("Sleeping 20 seconds to allow connection reaper to clean up connections " +
                    "and for the pools to recover.");
            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);

            Collection<SocketChannel> openChannels = channelMonitor.openChannels(nodes);
            assertThat(openChannels.size())
                    .as("Number of open connections does not meet expected: %s", openChannels)
                    .isEqualTo(maxConnections);

            session.close();

            openChannels = channelMonitor.openChannels(nodes);
            assertThat(openChannels.size())
                    .as("Number of open connections does not meet expected: %s", openChannels)
                    .isEqualTo(1);

            workerPool.shutdown();
        }
    }

    /**
     * Creates the testks schema with a 'record' table and preloads 30k records.
     */
    private static void setupSchema(Session session) throws InterruptedException, ExecutionException, TimeoutException {
        logger.debug("Creating keyspace");
        session.execute("create KEYSPACE " + KEYSPACE +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}");
        session.execute("use " + KEYSPACE);

        logger.debug("Creating table");
        session.execute("create table record (\n"
                + "  name text,\n"
                + "  phone text,\n"
                + "  value text,\n"
                + "  PRIMARY KEY (name, phone)\n"
                + ");");

        int records = 30000;
        PreparedStatement insertStmt = session.prepare("insert into record (name, phone, value) values (?, ?, ?)");

        for (int i = 0; i < records; i++) {
            if (i % 1000 == 0)
                logger.debug("Inserting record {}.", i);
            session.execute(insertStmt.bind("0", Integer.toString(i), "test"));
        }
        logger.debug("Inserts complete.");
    }

    public static class TimeoutStressWorker implements Runnable {

        private final Semaphore concurrentQueries;
        private final AtomicBoolean stopped;
        private final Session session;
        private final PreparedStatement statement;

        public TimeoutStressWorker(Session session, PreparedStatement statement, Semaphore concurrentQueries, AtomicBoolean stopped) {
            this.session = session;
            this.statement = statement;
            this.concurrentQueries = concurrentQueries;
            this.stopped = stopped;
        }


        @Override
        public void run() {

            while(!stopped.get()) {
                try {
                    concurrentQueries.acquire();
                    ResultSetFuture future = session.executeAsync(statement.bind("0"));
                    Futures.addCallback(future, new FutureCallback<ResultSet>() {

                        @Override
                        public void onSuccess(ResultSet result) {
                            concurrentQueries.release();
                            if(executedQueries.incrementAndGet() % 1000 == 0)
                                logger.debug("Successfully executed {}.  rows: {}", executedQueries.get(), result.getAvailableWithoutFetching());
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            concurrentQueries.release();
                            if(t instanceof NoHostAvailableException) {
                                //logger.error("NHAE: {}", t.getMessage());
                            } else {
                                //logger.error("Exception", t);
                            }
                        }
                    });
                } catch(Exception e) {
                    logger.error("Failure while submitting query.", e);
                }
            }

        }
    }
}
