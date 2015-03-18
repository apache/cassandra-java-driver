package com.datastax.driver.core;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class SessionStressTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final Logger logger = LoggerFactory.getLogger(SessionStressTest.class);

    private ExecutorService executorService;

    public SessionStressTest() {
        // 8 threads should be enough so that we stress the driver and not the OS thread scheduler
        executorService = Executors.newFixedThreadPool(8);
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return new ArrayList<String>(0);
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
        cluster = Cluster.builder().addContactPoints(CCMBridge.IP_PREFIX + '1')
            .withPoolingOptions(new PoolingOptions().setCoreConnectionsPerHost(HostDistance.LOCAL, 1)).build();

        // The cluster has not been initialized yet, therefore the control connection is not opened
        assertEquals(cluster.manager.sessions.size(), 0);
        assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 0);

        // The first session initializes the cluster and its control connection
        // This is a local cluster so we also have 2 connections per session
        Session session = cluster.connect();
        assertEquals(cluster.manager.sessions.size(), 1);
        int coreConnections = TestUtils.numberOfLocalCoreConnections(cluster);
        assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1 + coreConnections);

        // Closing the session keeps the control connection opened
        session.close();
        assertEquals(cluster.manager.sessions.size(), 0);
        assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1);

        try {
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
                assertEquals(cluster.manager.sessions.size(), nbOfSessions);
                assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(),
                             coreConnections * nbOfSessions + 1);

                // Close half of the sessions asynchronously
                logger.info("Closing {}/{} sessions.", halfOfTheSessions, nbOfSessions);
                waitFor(closeSessionsConcurrently(halfOfTheSessions));

                // Check that we have the right number of sessions and connections
                assertEquals(cluster.manager.sessions.size(), halfOfTheSessions);
                assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(),
                             coreConnections * (nbOfSessions / 2) + 1);

                // Close and open the same number of sessions concurrently
                logger.info("Closing and Opening {} sessions concurrently.", halfOfTheSessions);
                CountDownLatch startSignal = new CountDownLatch(1);
                List<Future<Session>> openSessionFutures = openSessionsConcurrently(halfOfTheSessions, startSignal);
                List<Future<CloseFuture>> closeSessionsFutures = closeSessionsConcurrently(halfOfTheSessions,
                        startSignal);
                startSignal.countDown();
                waitFor(openSessionFutures);
                waitFor(closeSessionsFutures);

                // Check that we have the same number of sessions and connections
                assertEquals(cluster.manager.sessions.size(), halfOfTheSessions);
                assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(),
                             coreConnections * (nbOfSessions / 2) + 1);

                // Close the remaining sessions
                logger.info("Closing remaining {} sessions.", halfOfTheSessions);
                waitFor(closeSessionsConcurrently(halfOfTheSessions));

                // Check that we have a clean state
                assertEquals(cluster.manager.sessions.size(), 0);
                assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1);

                // On OSX, the TCP connections are released after 15s by default (sysctl -a net.inet.tcp.msl)
                logger.info("Sleeping {} seconds so that TCP connections are released by the OS", sleepTime);
                Uninterruptibles.sleepUninterruptibly(sleepTime, TimeUnit.SECONDS);
            }
        } finally {
            cluster.close();
        }
    }

    private List<Future<Session>> openSessionsConcurrently(int iterations) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        return openSessionsConcurrently(iterations, countDownLatch);
    }

    private List<Future<Session>> openSessionsConcurrently(int iterations, CountDownLatch countDownLatch) {
        // Open new sessions once all tasks have been created
        List<Future<Session>> sessionFutures = new ArrayList<Future<Session>>(iterations);
        for (int i = 0; i < iterations; i++) {
            sessionFutures.add(executorService.submit(new OpenSession(cluster, countDownLatch)));
        }
        countDownLatch.countDown();
        return sessionFutures;
    }

    private List<Future<CloseFuture>> closeSessionsConcurrently(int iterations) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        return closeSessionsConcurrently(iterations, countDownLatch);
    }

    private List<Future<CloseFuture>> closeSessionsConcurrently(int iterations, CountDownLatch countDownLatch) {
        // Get a reference to every session we want to close
        Stack<Session> sessionsToClose = new Stack<Session>();
        Iterator<? extends Session> iterator = cluster.manager.sessions.iterator();
        for (int i = 0; i < iterations; i++) {
            sessionsToClose.push(iterator.next());
        }

        // Close sessions asynchronously once all tasks have been created
        List<Future<CloseFuture>> closeFutures = new ArrayList<Future<CloseFuture>>(iterations);
        for (int i = 0; i < iterations; i++) {
            closeFutures.add(executorService.submit(new CloseSession(sessionsToClose.pop(), countDownLatch)));
        }
        countDownLatch.countDown();
        return closeFutures;
    }

    private <E> void waitFor(List<Future<E>> futures) {
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

    private static class OpenSession implements Callable<Session> {
        private final Cluster cluster;
        private final CountDownLatch startSignal;

        OpenSession(Cluster cluster, CountDownLatch startSignal) {
            this.cluster = cluster;
            this.startSignal = startSignal;
        }

        @Override
        public Session call() throws Exception {
            startSignal.await();
            return cluster.connect();
        }
    }

    private static class CloseSession implements Callable<CloseFuture> {
        private final Session session;
        private final CountDownLatch startSignal;

        CloseSession(Session session, CountDownLatch startSignal) {
            this.session = session;
            this.startSignal = startSignal;
        }

        @Override
        public CloseFuture call() throws Exception {
            startSignal.await();
            return session.closeAsync();
        }
    }
}
