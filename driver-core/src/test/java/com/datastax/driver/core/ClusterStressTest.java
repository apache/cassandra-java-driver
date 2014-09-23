package com.datastax.driver.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

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
        for (int i = 0; i < 500; i++) {
            List<Cluster> clusters = waitFor(createClustersConcurrently(numberOfClusters));
            waitFor(closeClustersConcurrently(clusters));
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

    private <E> List<E> waitFor(List<Future<E>> futures) {
        List<E> result = new ArrayList<E>(futures.size());
        for (Future<E> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while waiting for future", e);
            } catch (ExecutionException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
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

            Cluster cluster = Cluster.builder().addContactPoints(CCMBridge.IP_PREFIX + '1').build();

            // The cluster has not been initialized yet, therefore the control connection is not opened
            assertEquals(cluster.manager.sessions.size(), 0);
            assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 0);

            // The first session initializes the cluster and its control connection
            // This is a local cluster so we also have 2 connections per session
            Session session = cluster.connect();
            assertEquals(cluster.manager.sessions.size(), 1);
            assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 3);

            // Closing the session keeps the control connection opened
            session.close();
            assertEquals(cluster.manager.sessions.size(), 0);
            assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1);

            return cluster;
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
}
