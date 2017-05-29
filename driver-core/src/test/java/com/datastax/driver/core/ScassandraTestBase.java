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

import org.scassandra.Scassandra;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.CurrentClient;
import org.scassandra.http.client.PrimingClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.fail;

/**
 * Base class for Scassandra tests.
 * This class takes care of starting and stopping a Scassandra server,
 * and provides some helper methods to leverage the creation of Cluster and Session objects.
 * The actual creation of such objects is however left to subclasses.
 * If a single cluster instance can be shared by all test methods,
 * consider using {@link com.datastax.driver.core.ScassandraTestBase.PerClassCluster} instead.
 */
public abstract class ScassandraTestBase {

    protected Scassandra scassandra;

    protected InetSocketAddress hostAddress;

    protected PrimingClient primingClient;

    protected ActivityClient activityClient;

    protected CurrentClient currentClient;

    protected static String ip = TestUtils.ipOfNode(1);

    @BeforeClass(groups = {"short", "long"})
    public void startScassandra() {
        scassandra = TestUtils.createScassandraServer();
        scassandra.start();
        primingClient = scassandra.primingClient();
        activityClient = scassandra.activityClient();
        currentClient = scassandra.currentClient();
        hostAddress = new InetSocketAddress(ip, scassandra.getBinaryPort());
    }

    @AfterClass(groups = {"short", "long"})
    public void stopScassandra() {
        if (scassandra != null)
            scassandra.stop();
    }

    @BeforeMethod(groups = {"short", "long"})
    @AfterMethod(groups = {"short", "long"})
    public void resetClients() {
        activityClient.clearAllRecordedActivity();
        primingClient.clearAllPrimes();
        currentClient.enableListener();
    }

    protected Cluster.Builder createClusterBuilder() {
        return Cluster.builder()
                .withPort(scassandra.getBinaryPort())
                .addContactPoints(hostAddress.getAddress())
                .withPort(scassandra.getBinaryPort())
                .withPoolingOptions(new PoolingOptions()
                        .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                        .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
                        .setHeartbeatIntervalSeconds(0));
    }

    protected Host retrieveSingleHost(Cluster cluster) {
        Host host = cluster.getMetadata().getHost(hostAddress);
        if (host == null) {
            fail("Unable to retrieve host");
        }
        return host;
    }

    /**
     * This subclass of ScassandraTestBase assumes that
     * the same Cluster instance will be used for all tests.
     */
    public static abstract class PerClassCluster extends ScassandraTestBase {

        protected Cluster cluster;

        protected Session session;

        protected Host host;

        @BeforeClass(groups = {"short", "long"}, dependsOnMethods = "startScassandra")
        public void initCluster() {
            Cluster.Builder builder = createClusterBuilder();
            cluster = builder.build();
            host = retrieveSingleHost(cluster);
            session = cluster.connect();
        }

        @AfterClass(groups = {"short", "long"}, alwaysRun = true)
        public void closeCluster() {
            if (cluster != null)
                cluster.close();
        }

        @AfterClass(groups = {"short", "long"}, dependsOnMethods = "closeCluster", alwaysRun = true)
        @Override
        public void stopScassandra() {
            super.stopScassandra();
        }

    }

}
