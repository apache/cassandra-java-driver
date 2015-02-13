package com.datastax.driver.core;

import java.net.InetSocketAddress;

import org.scassandra.Scassandra;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

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

    private static final Logger logger = LoggerFactory.getLogger(ScassandraTestBase.class);

    protected Scassandra scassandra;

    protected InetSocketAddress hostAddress;

    protected PrimingClient primingClient;

    protected ActivityClient activityClient;

    @BeforeClass(groups = { "short", "long" })
    public void startScassandra() {
        scassandra = TestUtils.createScassandraServer();
        scassandra.start();
        primingClient = scassandra.primingClient();
        activityClient = scassandra.activityClient();
        hostAddress = new InetSocketAddress("127.0.0.1", scassandra.getBinaryPort());
    }

    @AfterClass(groups = { "short", "long" })
    public void stopScassandra() {
        if(scassandra != null)
            scassandra.stop();
    }

    @BeforeMethod(groups = { "short", "long" })
    @AfterMethod(groups = { "short", "long" })
    public void resetClients() {
        activityClient.clearAllRecordedActivity();
        primingClient.clearAllPrimes();
    }

    protected Cluster.Builder createClusterBuilder() {
        Cluster.Builder builder = Cluster.builder()
            .addContactPoint("127.0.0.1")
            .withPoolingOptions(new PoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
                .setHeartbeatIntervalSeconds(0))
            .withPort(scassandra.getBinaryPort());
        return builder;
    }

    protected Host retrieveSingleHost(Cluster cluster) {
        Host host = cluster.getMetadata().getHost(hostAddress);
        if(host == null) {
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

        @BeforeClass(groups = { "short", "long" }, dependsOnMethods = "startScassandra")
        public void initCluster() {
            Cluster.Builder builder = createClusterBuilder();
            cluster = builder.build();
            host = retrieveSingleHost(cluster);
            session = cluster.connect();
        }

        @AfterClass(groups = { "short", "long" })
        public void closeCluster() {
            if (cluster != null)
                cluster.close();
        }

        @AfterClass(groups = { "short", "long" }, dependsOnMethods = "closeCluster")
        @Override
        public void stopScassandra() {
            super.stopScassandra();
        }

    }

}