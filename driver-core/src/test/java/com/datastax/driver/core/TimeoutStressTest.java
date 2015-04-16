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
import com.google.common.base.Predicate;
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

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static org.assertj.core.api.Assertions.assertThat;

public class TimeoutStressTest {

    static final Logger logger = LoggerFactory.getLogger(TimeoutStressTest.class);

    // Maximum number of concurrent queries running at a given time.
    static final int CONCURRENT_QUERIES = 25;

    // How long the test should run for, may want to consider running for longer periods to time to check for leaks
    // that could occur over very tiny timing windows.
    static final long DURATION = 240000;

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

    static PreparedStatement statement;

    static final Predicate<Connection> OPEN_CONNECTIONS = new Predicate<Connection>() {
        @Override
        public boolean apply(Connection input) {
            // Capture Connections that have inFlight requests.  These will be eventually closed
            // but potentially remain open.
            return input.inFlight.get() > 0;
        }
    };

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
        Session session = cluster.connect();
        setupSchema(session);
        session.close();
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

    @Test(groups = "long")
    public void host_state_should_be_maintained_with_timeouts() {
        // Set very low timeouts.
        cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(CONNECTION_TIMEOUT_IN_MS);
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(READ_TIMEOUT_IN_MS);
        Session session = cluster.connect();

        int workers = Runtime.getRuntime().availableProcessors();
        ExecutorService workerPool = Executors.newFixedThreadPool(workers,
                new ThreadFactoryBuilder().setNameFormat("timeout-stress-test-worker-%d").setDaemon(true).build());

        AtomicBoolean stopped = new AtomicBoolean(false);

        // Ensure that we never exceed MaxConnectionsPerHost * nodes + 1 control connection.
        int maxConnections = cluster.getConfiguration()
                .getPoolingOptions().getMaxConnectionsPerHost(HostDistance.LOCAL) * nodes.size() + 1;

        try {
            Semaphore concurrentQueries = new Semaphore(CONCURRENT_QUERIES);
            for (int i = 0; i < workers; i++) {
                workerPool.submit(new TimeoutStressWorker(session, concurrentQueries, stopped));
            }

            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < DURATION) {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                channelMonitor.report();
                // Some connections that are being closed may have had active requests which are delegated to the
                // reaper for cleanup later.
                Collection<SocketChannel> openChannels = channelMonitor.openChannels(nodes);

                int maximumExpected = maxConnections + openConnectionsInReaper();
                // Ensure that we don't exceed maximum connections.  Log as warning as there will be a bit of a timing
                // factor between retrieving open connections and checking the reaper.
                if(openChannels.size() > maximumExpected) {
                    logger.warn("{} of open channels: {} exceeds maximum expected: {}", openChannels.size(),
                            maximumExpected, openChannels);
                }
            }
        } finally {
            stopped.set(true);

            logger.debug("Sleeping 20 seconds to allow connection reaper to clean up connections.");
            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);

            Collection<SocketChannel> openChannels = channelMonitor.openChannels(nodes);
            assertThat(openChannels.size())
                    .as("Number of open connections does not meet expected: %s", openChannels)
                    .isLessThanOrEqualTo(maxConnections);

            session.close();

            logger.debug("Sleeping 20 seconds to allow connection reaper to clean up connections.");
            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);

            openChannels = channelMonitor.openChannels(nodes);
            assertThat(openChannels.size())
                    .as("Number of open connections does not meet expected: %s", openChannels)
                    .isEqualTo(1);

            workerPool.shutdown();
        }
    }

    private int openConnectionsInReaper() {
        return size(filter(Lists.newArrayList(cluster.manager.reaper.connections.keySet()), OPEN_CONNECTIONS));
    }

    /**
     * Creates the testks schema with a 'record' table and preloads 30k records.
     */
    private static void setupSchema(Session session) throws InterruptedException, ExecutionException, TimeoutException {
        logger.debug("Creating keyspace");
        session.execute("create KEYSPACE if not exists " + KEYSPACE +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}");
        session.execute("use " + KEYSPACE);

        logger.debug("Creating table");
        session.execute("create table if NOT EXISTS record (\n"
                + "  name text,\n"
                + "  phone text,\n"
                + "  value text,\n"
                + "  PRIMARY KEY (name, phone)\n"
                + ");");

        int records = 30000;
        List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(records);

        for (int i = 0; i < records; i++) {
            if (i % 1000 == 0)
                logger.debug("Inserting record {}.", i);
            futures.add(session.executeAsync(
                    "insert into record (name, phone, value) values (?, ?, ?)", "0", Integer.toString(i), "test"));
        }

        statement = session.prepare("select * from " + KEYSPACE + ".record where name=? limit 1000;");

        logger.debug("Completed inserting data.  Waiting up to 30 for inserts to complete seconds before proceeding.");
        Futures.allAsList(futures).get(30, TimeUnit.SECONDS);
        logger.debug("Inserts complete.");
    }

    public static class TimeoutStressWorker implements Runnable {

        private final Semaphore concurrentQueries;
        private final AtomicBoolean stopped;
        private final Session session;

        public TimeoutStressWorker(Session session, Semaphore concurrentQueries, AtomicBoolean stopped) {
            this.session = session;
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
