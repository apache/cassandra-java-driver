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
package com.datastax.driver.core.policies;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.mockito.Mockito;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.times;

import com.datastax.driver.core.*;

/**
 * Base class for retry policy integration tests.
 *
 * We use SCassandra to easily simulate specific errors (unavailable, read timeout...) on nodes,
 * and SortingLoadBalancingPolicy to get a predictable order of the query plan (always host1, host2, host3).
 *
 * Note that SCassandra only allows a limited number of test cases, for instance it always returns errors
 * with receivedResponses = 0. If that becomes more finely tuneable in the future, we'll be able to add more
 * tests in child classes.
 */
public class AbstractRetryPolicyIntegrationTest {
    protected SCassandraCluster scassandras;
    protected Cluster cluster = null;
    protected Metrics.Errors errors;
    protected Host host1, host2, host3;
    protected Session session;

    protected RetryPolicy retryPolicy;

    protected AbstractRetryPolicyIntegrationTest(RetryPolicy retryPolicy) {
        this.retryPolicy = Mockito.spy(retryPolicy);
    }

    @BeforeClass(groups = "short")
    public void beforeClass() {
        scassandras = new SCassandraCluster(CCMBridge.IP_PREFIX, 3);
    }

    @BeforeMethod(groups = "short")
    public void beforeMethod() {
        cluster = Cluster.builder()
            .addContactPoint(CCMBridge.ipOfNode(1))
            .withRetryPolicy(retryPolicy)
            .withLoadBalancingPolicy(new SortingLoadBalancingPolicy())
            .build();

        session = cluster.connect();

        host1 = TestUtils.findHost(cluster, 1);
        host2 = TestUtils.findHost(cluster, 2);
        host3 = TestUtils.findHost(cluster, 3);

        errors = cluster.getMetrics().getErrorMetrics();

        Mockito.reset(retryPolicy);
        scassandras.clearAllPrimes();
        scassandras.clearAllRecordedActivity();
    }

    protected void simulateError(int hostNumber, PrimingRequest.Result result) {
        scassandras
            .prime(hostNumber, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withResult(result)
                .build());
    }

    protected void simulateNormalResponse(int hostNumber) {
        scassandras
            .prime(hostNumber, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withRows(row("result", "result1"))
                .build());
    }

    private static List<Map<String, ?>> row(String key, String value) {
        return ImmutableList.<Map<String, ?>>of(ImmutableMap.of(key, value));
    }

    protected ResultSet query() {
        return query(session);
    }

    protected ResultSet query(Session session) {
        return session.execute("mock query");
    }

    protected void assertOnReadTimeoutWasCalled(int times) {
        Mockito.verify(retryPolicy, times(times)).onReadTimeout(
            any(Statement.class), any(ConsistencyLevel.class), anyInt(), anyInt(), anyBoolean(), anyInt());

    }

    protected void assertOnWriteTimeoutWasCalled(int times) {
        Mockito.verify(retryPolicy, times(times)).onWriteTimeout(
            any(Statement.class), any(ConsistencyLevel.class), any(WriteType.class), anyInt(), anyInt(), anyInt());
    }

    protected void assertOnUnavailableWasCalled(int times) {
        Mockito.verify(retryPolicy, times(times)).onUnavailable(
            any(Statement.class), any(ConsistencyLevel.class), anyInt(), anyInt(), anyInt());
    }

    protected void assertQueried(int hostNumber, int times) {
        assertThat(scassandras.retrieveQueries(hostNumber)).hasSize(times);
    }

    @AfterMethod(groups = "short")
    public void afterMethod() {
        scassandras.clearAllPrimes();
        if (cluster != null)
            cluster.close();
    }

    @AfterClass(groups = "short")
    public void afterClass() {
        if (scassandras != null)
            scassandras.stop();
    }
}
