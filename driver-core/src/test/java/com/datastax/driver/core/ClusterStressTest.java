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
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class ClusterStressTest extends CCMTestsSupport {

    private static final Logger logger = LoggerFactory.getLogger(ClusterStressTest.class);

    private ExecutorService executorService;

    public ClusterStressTest() {
        // 8 threads should be enough so that we stress the driver and not the OS thread scheduler
        executorService = Executors.newFixedThreadPool(8);
    }

    @Test(groups = "long")
    public void clusters_should_not_leak_connections() {
        int numberOfClusters = 10;
        int numberOfIterations = 500;
        try {
            for (int i = 1; i < numberOfIterations; i++) {
                logger.info("On iteration {}/{}.", i, numberOfIterations);
                logger.info("Creating {} clusters", numberOfClusters);
                List<CreateClusterAndCheckConnections> actions =
                        waitForCreates(createClustersConcurrently(numberOfClusters));
                waitForCloses(closeClustersConcurrently(actions));
                if (logger.isDebugEnabled())
                    logger.debug("# {} threads currently running", Thread.getAllStackTraces().keySet().size());
            }
        } finally {
            logger.info("Sleeping 60 seconds to free TCP resources");
            Uninterruptibles.sleepUninterruptibly(60, TimeUnit.SECONDS);
        }
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

    private List<Future<CreateClusterAndCheckConnections>> createClustersConcurrently(int numberOfClusters) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        return createClustersConcurrently(numberOfClusters, countDownLatch);
    }

    private List<Future<CreateClusterAndCheckConnections>> createClustersConcurrently(int numberOfClusters,
                                                                                      CountDownLatch countDownLatch) {
        List<Future<CreateClusterAndCheckConnections>> clusterFutures =
                Lists.newArrayListWithCapacity(numberOfClusters);
        for (int i = 0; i < numberOfClusters; i++) {
            clusterFutures.add(executorService.submit(new CreateClusterAndCheckConnections(countDownLatch)));
        }
        countDownLatch.countDown();
        return clusterFutures;
    }

    private List<Future<Void>> closeClustersConcurrently(List<CreateClusterAndCheckConnections> actions) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        return closeClustersConcurrently(actions, countDownLatch);
    }

    private List<Future<Void>> closeClustersConcurrently(List<CreateClusterAndCheckConnections> actions,
                                                         CountDownLatch startSignal) {
        List<Future<Void>> closeFutures = Lists.newArrayListWithCapacity(actions.size());
        for (CreateClusterAndCheckConnections action : actions) {
            closeFutures.add(executorService.submit(new CloseCluster(action.cluster, action.channelMonitor,
                    startSignal)));
        }
        startSignal.countDown();
        return closeFutures;
    }

    private List<CreateClusterAndCheckConnections> waitForCreates(
            List<Future<CreateClusterAndCheckConnections>> futures) {
        List<CreateClusterAndCheckConnections> actions = Lists.newArrayListWithCapacity(futures.size());
        // If an error occurs, we will abort the test, but we still want to close all the clusters
        // that were opened successfully, so we iterate over the whole list no matter what.
        AssertionError error = null;
        for (Future<CreateClusterAndCheckConnections> future : futures) {
            try {
                actions.add(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (error == null)
                    error = assertionError("Interrupted while waiting for future creation", e);
            } catch (ExecutionException e) {
                if (error == null) {
                    Throwable cause = e.getCause();
                    if (cause instanceof AssertionError)
                        error = (AssertionError) cause;
                    else
                        error = assertionError("Error while creating a cluster", cause);
                }
            }
        }
        if (error != null) {
            for (CreateClusterAndCheckConnections action : actions)
                action.cluster.close();
            throw error;
        } else
            return actions;
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
                        error = (AssertionError) cause;
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

    private class CreateClusterAndCheckConnections implements Callable<CreateClusterAndCheckConnections> {
        private final CountDownLatch startSignal;
        private Cluster cluster;
        private final SocketChannelMonitor channelMonitor = new SocketChannelMonitor();

        CreateClusterAndCheckConnections(CountDownLatch startSignal) {
            this.startSignal = startSignal;
            this.cluster = Cluster.builder()
                    .addContactPoints(getContactPoints())
                    .withPort(ccm().getBinaryPort())
                    .withPoolingOptions(new PoolingOptions().setCoreConnectionsPerHost(HostDistance.LOCAL, 1))
                    .withNettyOptions(channelMonitor.nettyOptions()).build();
        }

        @Override
        public CreateClusterAndCheckConnections call() throws Exception {
            startSignal.await();

            try {
                // There should be 1 control connection after initializing.
                cluster.init();
                assertEquals(cluster.manager.sessions.size(), 0);
                assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1);
                assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(), 1);

                // The first session initializes the cluster and its control connection
                Session session = cluster.connect();
                assertEquals(cluster.manager.sessions.size(), 1);
                assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1 + TestUtils.numberOfLocalCoreConnections(cluster));
                assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(), 1 + TestUtils.numberOfLocalCoreConnections(cluster));

                // Closing the session keeps the control connection opened
                session.close();
                assertEquals(cluster.manager.sessions.size(), 0);
                assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1);
                assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(), 1);

                return this;
            } catch (AssertionError e) {
                // If an assertion fails, close the cluster now, because it's the last time we
                // have a reference to it.
                cluster.close();
                cluster = null;
                throw e;
            } finally {
                channelMonitor.stop();
            }
        }
    }

    private class CloseCluster implements Callable<Void> {
        private Cluster cluster;
        private SocketChannelMonitor channelMonitor;
        private final CountDownLatch startSignal;

        CloseCluster(Cluster cluster, SocketChannelMonitor channelMonitor, CountDownLatch startSignal) {
            this.cluster = cluster;
            this.channelMonitor = channelMonitor;
            this.startSignal = startSignal;
        }

        @Override
        public Void call() throws Exception {
            startSignal.await();
            try {
                cluster.close();
                assertEquals(cluster.manager.sessions.size(), 0);
                assertEquals(channelMonitor.openChannels(getContactPointsWithPorts()).size(), 0);
            } finally {
                channelMonitor.stop();
                cluster = null;
                channelMonitor = null;
            }
            return null;
        }
    }

    private static AssertionError assertionError(String message, Throwable cause) {
        AssertionError error = new AssertionError(message);
        error.initCause(cause);
        return error;
    }
}
