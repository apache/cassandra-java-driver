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

import com.datastax.driver.core.Metrics.Errors;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision;
import org.testng.annotations.Test;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class MetricsTest extends CCMTestsSupport {
    private volatile RetryDecision retryDecision;

    MBeanServer server = ManagementFactory.getPlatformMBeanServer();

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withRetryPolicy(new RetryPolicy() {
            @Override
            public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
                return retryDecision;
            }

            @Override
            public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
                return retryDecision;
            }

            @Override
            public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
                return retryDecision;
            }

            @Override
            public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
                return retryDecision;
            }

            @Override
            public void init(Cluster cluster) {
            }

            @Override
            public void close() {
            }
        });
    }

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE test (k int primary key, v int)",
                "INSERT INTO test (k, v) VALUES (1, 1)");
    }

    @Test(groups = "short")
    public void retriesTest() {
        retryDecision = RetryDecision.retry(ConsistencyLevel.ONE);

        // We only have one node, this will throw an unavailable exception
        Statement statement = new SimpleStatement("SELECT v FROM test WHERE k = 1").setConsistencyLevel(ConsistencyLevel.TWO);
        session().execute(statement);

        Errors errors = cluster().getMetrics().getErrorMetrics();
        assertEquals(errors.getUnavailables().getCount(), 1);
        assertEquals(errors.getRetries().getCount(), 1);
        assertEquals(errors.getRetriesOnUnavailable().getCount(), 1);

        retryDecision = RetryDecision.ignore();
        session().execute(statement);

        assertEquals(errors.getUnavailables().getCount(), 2);
        assertEquals(errors.getIgnores().getCount(), 1);
        assertEquals(errors.getIgnoresOnUnavailable().getCount(), 1);
    }

    /**
     * Validates that metrics are enabled and exposed by JMX by default by checking that
     * {@link Cluster#getMetrics()} is not null and 'clusterName-metrics:name=connected-to'
     * MBean is present.
     *
     * @test_category metrics
     */
    @Test(groups = "short")
    public void should_enable_metrics_and_jmx_by_default() throws Exception {
        assertThat(cluster().getMetrics()).isNotNull();
        ObjectName clusterMetricsON = ObjectName.getInstance(cluster().getClusterName() + "-metrics:name=connected-to");
        MBeanInfo mBean = server.getMBeanInfo(clusterMetricsON);
        assertThat(mBean).isNotNull();

        assertThat(cluster().getConfiguration().getMetricsOptions().isEnabled()).isTrue();
        assertThat(cluster().getConfiguration().getMetricsOptions().isJMXReportingEnabled()).isTrue();
    }

    /**
     * Validates that when metrics are disabled using {@link Cluster.Builder#withoutMetrics()}
     * that {@link Cluster#getMetrics()} returns null and 'clusterName-metrics:name=connected-to'
     * MBean is not present.
     *
     * @test_category metrics
     */
    @Test(groups = "short", expectedExceptions = InstanceNotFoundException.class)
    public void metrics_should_be_null_when_metrics_disabled() throws Exception {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withoutMetrics()
                .build());
        try {
            cluster.init();
            assertThat(cluster.getMetrics()).isNull();
            assertThat(cluster.getConfiguration().getMetricsOptions().isEnabled()).isFalse();
            ObjectName clusterMetricsON = ObjectName.getInstance(cluster.getClusterName() + "-metrics:name=connected-to");
            server.getMBeanInfo(clusterMetricsON);
        } finally {
            cluster.close();
        }
    }

    /**
     * Validates that when metrics are enabled but JMX reporting is disabled via
     * {@link Cluster.Builder#withoutJMXReporting()} that {@link Cluster#getMetrics()}
     * is not null and 'clusterName-metrics:name=connected-to' MBean is present.
     *
     * @test_category metrics
     */
    @Test(groups = "short", expectedExceptions = InstanceNotFoundException.class)
    public void should_be_no_jmx_mbean_when_jmx_is_disabled() throws Exception {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withoutJMXReporting()
                .build());
        try {
            cluster.init();
            assertThat(cluster.getMetrics()).isNotNull();
            assertThat(cluster.getConfiguration().getMetricsOptions().isEnabled()).isTrue();
            assertThat(cluster.getConfiguration().getMetricsOptions().isJMXReportingEnabled()).isFalse();
            ObjectName clusterMetricsON = ObjectName.getInstance(cluster.getClusterName() + "-metrics:name=connected-to");
            server.getMBeanInfo(clusterMetricsON);
        } finally {
            cluster.close();
        }
    }
}
