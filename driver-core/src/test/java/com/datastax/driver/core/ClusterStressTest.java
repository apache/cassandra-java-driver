package com.datastax.driver.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ClusterStressTest extends CCMBridge.PerClassSingleNodeCluster {
    private static final Logger logger = LoggerFactory.getLogger(ClusterStressTest.class);

    private ExecutorService executorService;

    public ClusterStressTest() {
        // 8 threads should be enough so that we stress the driver and not the OS thread scheduler
        executorService = Executors.newFixedThreadPool(8);
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return new ArrayList<String>(0);
    }

    @Test(groups = "long")
    public void clusters_should_not_leak_connections() {
        int numberOfClusters = 10;
        int numberOfIterations = 500;
        for (int i = 1; i < numberOfIterations; i++) {
            logger.info("On iteration {}/{}.", i, numberOfIterations);
            logger.info("Creating {} clusters", numberOfClusters);
            List<Cluster> clusters = waitForCreates(createClustersConcurrently(numberOfClusters));
            waitForCloses(closeClustersConcurrently(clusters));
            logger.debug("# {} threads currently running", Thread.getAllStackTraces().keySet().size());
        }
    }

    private List<Future<Cluster>> createClustersConcurrently(int numberOfClusters) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        return createClustersConcurrently(numberOfClusters, countDownLatch);
    }

    private List<Future<Cluster>> createClustersConcurrently(int numberOfClusters, CountDownLatch countDownLatch) {
        List<Future<Cluster>> clusterFutures = new ArrayList<Future<Cluster>>(numberOfClusters);
        for (int i = 0; i < numberOfClusters; i++) {
            clusterFutures.add(executorService.submit(new CreateClusterAndCheckConnections(countDownLatch)));
        }
        countDownLatch.countDown();
        return clusterFutures;
    }

    private List<Future<Void>> closeClustersConcurrently(List<Cluster> clusters) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        return closeClustersConcurrently(clusters, countDownLatch);
    }

    private List<Future<Void>> closeClustersConcurrently(List<Cluster> clusters, CountDownLatch startSignal) {
        List<Future<Void>> closeFutures = new ArrayList<Future<Void>>(clusters.size());
        for (Cluster cluster : clusters) {
            closeFutures.add(executorService.submit(new CloseCluster(cluster, startSignal)));
        }
        startSignal.countDown();
        return closeFutures;
    }

    private List<Cluster> waitForCreates(List<Future<Cluster>> futures) {
        List<Cluster> clusters = new ArrayList<Cluster>(futures.size());
        // If an error occurs, we will abort the test, but we still want to close all the clusters
        // that were opened successfully, so we iterate over the whole list no matter what.
        AssertionError error = null;
        for (Future<Cluster> future : futures) {
            try {
                clusters.add(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (error == null)
                    error = assertionError("Interrupted while waiting for future creation", e);
            } catch (ExecutionException e) {
                if (error == null) {
                    Throwable cause = e.getCause();
                    if (cause instanceof AssertionError)
                        error = (AssertionError)cause;
                    else
                        error = assertionError("Error while creating a cluster", cause);
                }
            }
        }
        if (error != null) {
            for (Cluster cluster : clusters)
                cluster.close();
            throw error;
        } else
            return clusters;
    }

    private List<Void> waitForCloses(List<Future<Void>> futures) {
        List<Void> result = new ArrayList<Void>(futures.size());
        AssertionError error = null;
        for (Future<Void> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (error == null)
                    error = assertionError("Interrupted while waiting for future", e);
            } catch (ExecutionException e) {
                if (error == null) {
                    Throwable cause = e.getCause();
                    if (cause instanceof AssertionError)
                        error = (AssertionError)cause;
                    else
                        error = assertionError("Error while closing a cluster", cause);
                }
            }
        }
        if (error != null)
            throw error;
        else
            return result;
    }

    private static class CreateClusterAndCheckConnections implements Callable<Cluster> {
        private final CountDownLatch startSignal;

        CreateClusterAndCheckConnections(CountDownLatch startSignal) {
            this.startSignal = startSignal;
        }

        @Override
        public Cluster call() throws Exception {
            startSignal.await();

            Cluster cluster = Cluster.builder().addContactPoints(CCMBridge.IP_PREFIX + '1')
                .withPoolingOptions(new PoolingOptions().setCoreConnectionsPerHost(HostDistance.LOCAL, 1)).build();

            try {
                // The cluster has not been initialized yet, therefore the control connection is not opened
                assertEquals(cluster.manager.sessions.size(), 0);
                assertEquals((int)cluster.getMetrics().getOpenConnections().getValue(), 0);

                // The first session initializes the cluster and its control connection
                Session session = cluster.connect();
                assertEquals(cluster.manager.sessions.size(), 1);
                assertEquals((int)cluster.getMetrics().getOpenConnections().getValue(), 1 + TestUtils.numberOfLocalCoreConnections(cluster));

                // Closing the session keeps the control connection opened
                session.close();
                assertEquals(cluster.manager.sessions.size(), 0);
                assertEquals((int)cluster.getMetrics().getOpenConnections().getValue(), 1);

                return cluster;
            } catch (AssertionError e) {
                // If an assertion fails, close the cluster now, because it's the last time we
                // have a reference to it.
                cluster.close();
                throw e;
            }
        }
    }

    private static class CloseCluster implements Callable<Void> {
        private final Cluster cluster;
        private final CountDownLatch startSignal;

        CloseCluster(Cluster cluster, CountDownLatch startSignal) {
            this.cluster = cluster;
            this.startSignal = startSignal;
        }

        @Override
        public Void call() throws Exception {
            startSignal.await();

            cluster.close();
            assertEquals(cluster.manager.sessions.size(), 0);
            assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 0);

            return null;
        }
    }

    private static AssertionError assertionError(String message, Throwable cause) {
        AssertionError error = new AssertionError(message);
        error.initCause(cause);
        return error;
    }
}
