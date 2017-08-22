package com.datastax.driver.core;

import org.scassandra.Scassandra;
import org.scassandra.http.client.PrimingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static org.scassandra.http.client.PrimingRequest.then;

public class MetricsInFlightTest {
    private ScassandraCluster sCluster;

    @BeforeMethod(groups = "short")
    public void setUp() {
        sCluster = ScassandraCluster.builder().withNodes(1).build();
        sCluster.init();
    }

    @AfterMethod(groups = "short")
    public void tearDown() {
        clearActivityLog();
        sCluster.stop();
    }

    public void clearActivityLog() {
        for (Scassandra node : sCluster.nodes()) {
            node.activityClient().clearAllRecordedActivity();
        }
    }

    public Cluster.Builder builder() {
        //Note: nonQuietClusterCloseOptions is used to speed up tests
        return Cluster.builder()
                .addContactPoints(sCluster.address(1).getAddress())
                .withPort(sCluster.getBinaryPort()).withNettyOptions(nonQuietClusterCloseOptions);
    }

    @Test(groups = "short")
    public void should_count_inflight_requests_metrics() {
        sCluster.node(1).primingClient().prime(PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withThen(then().withFixedDelay(100000L))
                .build()
        );

        Cluster cluster = null;
        try {
            cluster = builder().build();
            Session session = cluster.connect();

            assertThat(cluster.getMetrics().getInFlightRequests().getValue()).isEqualTo(0);
            session.executeAsync("mock query");
            session.executeAsync("mock query");
            assertThat(cluster.getMetrics().getInFlightRequests().getValue()).isEqualTo(2);

        } finally {
            if (cluster != null) {
                cluster.close();
            }
        }
    }


    @Test(groups = "short")
    public void should_countdown_inflight_requests_metrics() {
        sCluster.node(1).primingClient().prime(PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withThen(then())
                .build()
        );

        Cluster cluster = null;
        try {
            cluster = builder().build();
            Session session = cluster.connect();

            assertThat(cluster.getMetrics().getInFlightRequests().getValue()).isEqualTo(0);
            session.executeAsync("mock query").getUninterruptibly();
            session.executeAsync("mock query").getUninterruptibly();
            assertThat(cluster.getMetrics().getInFlightRequests().getValue()).isEqualTo(0);

        } finally {
            if (cluster != null) {
                cluster.close();
            }
        }
    }

}
