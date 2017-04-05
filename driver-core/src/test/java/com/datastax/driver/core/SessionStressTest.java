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

import com.datastax.driver.core.utils.SocketChannelMonitor;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@CCMConfig(dirtiesContext = true)
public class SessionStressTest extends CCMTestsSupport {

    private static final Logger logger = LoggerFactory.getLogger(SessionStressTest.class);

    private ListeningExecutorService executorService;

    private Cluster stressCluster;

    private final SocketChannelMonitor channelMonitor = new SocketChannelMonitor();

    public SessionStressTest() {
        // 8 threads should be enough so that we stress the driver and not the OS thread scheduler
        executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(8));
    }

    @AfterMethod(groups = "long", alwaysRun = true)
    public void shutdown() throws Exception {
        executorService.shutdown();
        try {
            boolean shutdown = executorService.awaitTermination(30, TimeUnit.SECONDS);
            if (!shutdown)
                fail("executor ran for longer than expected");
        } catch (InterruptedException e) {
            fail("Interrupted while waiting for executor to shutdown");
        } finally {
            executorService = null;
            System.gc();
        }
    }

    /**
     * Stress test on opening/closing sessions.
     * <p/>
     * This test opens and closes {@code Session} in a multithreaded environment and makes sure that there is not
     * connection leak. More specifically, this test performs the following steps:
     * <p/>
     * <ul>
     * <li>Open 2000 {@code Session} concurrently</li>
     * <li>Verify that 2000 sessions are reported as open by the {@code Cluster}</li>
     * <li>Verify that 4001 connections are reported as open by the {@code Cluster}</li>
     * <li>Close 1000 {@code Session} concurrently</li>
     * <li>Verify that 1000 sessions are reported as open by the {@code Cluster}</li>
     * <li>Verify that 2001 connections are reported as open by the {@code Cluster}</li>
     * <li>Open concurrently 1000 {@code Session} while 1000 other {@code Session} are closed concurrently</li>
     * <li>Verify that 1000 sessions are reported as open by the {@code Cluster}</li>
     * <li>Verify that 2001 connections are reported as open by the {@code Cluster}</li>
     * <li>Close 1000 {@code Session} concurrently</li>
     * <li>Verify that 0 sessions are reported as open by the {@code Cluster}</li>
     * <li>Verify that 1 connection is reported as open by the {@code Cluster}</li>
     * </ul>
     * <p/>
     * This test is linked to JAVA-432.
     */
    @Test(groups = "long")
    public void sessions_should_not_leak_connections() {
        // override inherited field with a new cluster object and ensure 0 sessions and connections
        channelMonitor.reportAtFixedInterval(1, TimeUnit.SECONDS);
        stressCluster = Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withPoolingOptions(new PoolingOptions().setCoreConnectionsPerHost(HostDistance.LOCAL, 1))
                .withNettyOptions(channelMonitor.nettyOptions()).build();

        try {
            stressCluster.init();

            // The cluster has been initialized, we should have 1 connection.
            assertEquals(stressCluster.manager.sessions.size(), 0);
            assertEquals((int) stressCluster.getMetrics().getOpenConnections().getValue(), 1);

            // The first session initializes the cluster and its control connection
            // This is a local cluster so we also have 2 connections per session
            Session session = stressCluster.connect();
            assertEquals(stressCluster.manager.sessions.size(), 1);
            int coreConnections = TestUtils.numberOfLocalCoreConnections(stressCluster);
            assertEquals((int) stressCluster.getMetrics().getOpenConnections().getValue(), 1 + coreConnections);
            assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(), 1 + coreConnections);

            // Closing the session keeps the control connection opened
            session.close();
            assertEquals(stressCluster.manager.sessions.size(), 0);
            assertEquals((int) stressCluster.getMetrics().getOpenConnections().getValue(), 1);
            assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(), 1);

            int nbOfSessions = 2000;
            int halfOfTheSessions = nbOfSessions / 2;
            int nbOfIterations = 5;
            int sleepTime = 20;

            for (int iteration = 1; iteration <= nbOfIterations; iteration++) {
                logger.info("On iteration {}/{}.", iteration, nbOfIterations);
                logger.info("Creating {} sessions.", nbOfSessions);
                waitFor(openSessionsConcurrently(nbOfSessions));

                // We should see the exact number of opened sessions
                // Since we have 2 connections per session, we should see 2 * sessions + control connection
                assertEquals(stressCluster.manager.sessions.size(), nbOfSessions);
                assertEquals((int) stressCluster.getMetrics().getOpenConnections().getValue(),
                        coreConnections * nbOfSessions + 1);
                assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(), coreConnections * nbOfSessions + 1);

                // Close half of the sessions asynchronously
                logger.info("Closing {}/{} sessions.", halfOfTheSessions, nbOfSessions);
                waitFor(closeSessionsConcurrently(halfOfTheSessions));

                // Check that we have the right number of sessions and connections
                assertEquals(stressCluster.manager.sessions.size(), halfOfTheSessions);
                assertEquals((int) stressCluster.getMetrics().getOpenConnections().getValue(),
                        coreConnections * (nbOfSessions / 2) + 1);
                assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(),
                        coreConnections * (nbOfSessions / 2) + 1);

                // Close and open the same number of sessions concurrently
                logger.info("Closing and Opening {} sessions concurrently.", halfOfTheSessions);
                CountDownLatch startSignal = new CountDownLatch(2);
                List<ListenableFuture<Session>> openSessionFutures =
                        openSessionsConcurrently(halfOfTheSessions, startSignal);
                List<ListenableFuture<Void>> closeSessionsFutures = closeSessionsConcurrently(halfOfTheSessions,
                        startSignal);
                startSignal.countDown();
                waitFor(openSessionFutures);
                waitFor(closeSessionsFutures);

                // Check that we have the same number of sessions and connections
                assertEquals(stressCluster.manager.sessions.size(), halfOfTheSessions);
                assertEquals((int) stressCluster.getMetrics().getOpenConnections().getValue(),
                        coreConnections * (nbOfSessions / 2) + 1);
                assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(),
                        coreConnections * (nbOfSessions / 2) + 1);

                // Close the remaining sessions
                logger.info("Closing remaining {} sessions.", halfOfTheSessions);
                waitFor(closeSessionsConcurrently(halfOfTheSessions));

                // Check that we have a clean state
                assertEquals(stressCluster.manager.sessions.size(), 0);
                assertEquals((int) stressCluster.getMetrics().getOpenConnections().getValue(), 1);
                assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(), 1);

                // On OSX, the TCP connections are released after 15s by default (sysctl -a net.inet.tcp.msl)
                logger.info("Sleeping {} seconds so that TCP connections are released by the OS", sleepTime);
                Uninterruptibles.sleepUninterruptibly(sleepTime, TimeUnit.SECONDS);
            }
        } finally {
            stressCluster.close();
            stressCluster = null;

            // Ensure no channels remain open.
            assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(), 0);

            channelMonitor.stop();
            channelMonitor.report();

            logger.info("Sleeping 60 extra seconds");
            Uninterruptibles.sleepUninterruptibly(60, TimeUnit.SECONDS);

        }
    }

    private List<ListenableFuture<Session>> openSessionsConcurrently(int iterations) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        return openSessionsConcurrently(iterations, countDownLatch);
    }

    private List<ListenableFuture<Session>> openSessionsConcurrently(int iterations, CountDownLatch countDownLatch) {
        // Open new sessions once all tasks have been created
        List<ListenableFuture<Session>> sessionFutures = Lists.newArrayListWithCapacity(iterations);
        for (int i = 0; i < iterations; i++) {
            sessionFutures.add(executorService.submit(new OpenSession(countDownLatch)));
        }
        countDownLatch.countDown();
        return sessionFutures;
    }

    private List<ListenableFuture<Void>> closeSessionsConcurrently(int iterations) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        return closeSessionsConcurrently(iterations, countDownLatch);
    }

    private List<ListenableFuture<Void>> closeSessionsConcurrently(int iterations, CountDownLatch countDownLatch) {
        // Get a reference to every session we want to close
        Stack<Session> sessionsToClose = new Stack<Session>();
        Iterator<? extends Session> iterator = stressCluster.manager.sessions.iterator();
        for (int i = 0; i < iterations; i++) {
            sessionsToClose.push(iterator.next());
        }

        // Close sessions asynchronously once all tasks have been created
        List<ListenableFuture<CloseFuture>> closeFutures = Lists.newArrayListWithCapacity(iterations);
        for (int i = 0; i < iterations; i++) {
            closeFutures.add(executorService.submit(new CloseSession(sessionsToClose.pop(), countDownLatch)));
        }
        countDownLatch.countDown();

        // Immediately wait for CloseFutures, this should be very quick since all this work does is call closeAsync.
        List<ListenableFuture<Void>> futures = Lists.newArrayListWithCapacity(iterations);
        for (ListenableFuture<CloseFuture> closeFuture : closeFutures) {
            try {
                futures.add(closeFuture.get());
            } catch (Exception e) {
                logger.error("Got interrupted exception while waiting on closeFuture.", e);
            }
        }
        return futures;
    }

    private <E> void waitFor(List<ListenableFuture<E>> futures) {
        for (Future<E> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while waiting for future", e);
            } catch (ExecutionException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    private class OpenSession implements Callable<Session> {
        private final CountDownLatch startSignal;

        OpenSession(CountDownLatch startSignal) {
            this.startSignal = startSignal;
        }

        @Override
        public Session call() throws Exception {
            startSignal.await();
            return stressCluster.connect();
        }
    }

    private static class CloseSession implements Callable<CloseFuture> {
        private Session session;
        private final CountDownLatch startSignal;

        CloseSession(Session session, CountDownLatch startSignal) {
            this.session = session;
            this.startSignal = startSignal;
        }

        @Override
        public CloseFuture call() throws Exception {
            startSignal.await();
            try {
                return session.closeAsync();
            } finally {
                session = null;
            }
        }
    }
}
