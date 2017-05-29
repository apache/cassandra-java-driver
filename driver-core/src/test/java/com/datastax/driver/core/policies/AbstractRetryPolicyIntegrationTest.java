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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.ServerError;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.mockito.Mockito;
import org.scassandra.Scassandra;
import org.scassandra.http.client.ClosedConnectionConfig.CloseType;
import org.scassandra.http.client.Config;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.PrimingRequest.PrimingRequestBuilder;
import org.scassandra.http.client.Result;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.times;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.overloaded;
import static org.scassandra.http.client.Result.server_error;

/**
 * Base class for retry policy integration tests.
 * <p/>
 * We use SCassandra to easily simulate specific errors (unavailable, read timeout...) on nodes,
 * and SortingLoadBalancingPolicy to get a predictable order of the query plan (always host1, host2, host3).
 * <p/>
 * Note that SCassandra only allows a limited number of test cases, for instance it always returns errors
 * with receivedResponses = 0. If that becomes more finely tuneable in the future, we'll be able to add more
 * tests in child classes.
 */
public class AbstractRetryPolicyIntegrationTest {
    protected ScassandraCluster scassandras;
    protected Cluster cluster = null;
    protected Metrics.Errors errors;
    protected Host host1, host2, host3;
    protected Session session;

    protected RetryPolicy retryPolicy;

    protected AbstractRetryPolicyIntegrationTest() {
    }

    protected AbstractRetryPolicyIntegrationTest(RetryPolicy retryPolicy) {
        setRetryPolicy(retryPolicy);
    }

    protected final void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = Mockito.spy(retryPolicy);
    }

    @BeforeMethod(groups = "short")
    public void beforeMethod() {
        scassandras = ScassandraCluster.builder().withNodes(3).build();
        scassandras.init();

        cluster = Cluster.builder()
                .addContactPoints(scassandras.address(1).getAddress())
                .withPort(scassandras.getBinaryPort())
                .withRetryPolicy(retryPolicy)
                .withLoadBalancingPolicy(new SortingLoadBalancingPolicy())
                .withPoolingOptions(new PoolingOptions()
                        .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                        .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
                        .setHeartbeatIntervalSeconds(0))
                .withNettyOptions(nonQuietClusterCloseOptions)
                // Mark everything as idempotent by default so RetryPolicy is exercised.
                .withQueryOptions(new QueryOptions().setDefaultIdempotence(true))
                .build();

        session = cluster.connect();

        host1 = TestUtils.findHost(cluster, 1);
        host2 = TestUtils.findHost(cluster, 2);
        host3 = TestUtils.findHost(cluster, 3);

        errors = cluster.getMetrics().getErrorMetrics();

        Mockito.reset(retryPolicy);

        for (Scassandra node : scassandras.nodes()) {
            node.activityClient().clearAllRecordedActivity();
        }
    }

    protected void simulateError(int hostNumber, Result result) {
        simulateError(hostNumber, result, null);
    }

    protected void simulateError(int hostNumber, Result result, Config config) {
        PrimingRequest.Then.ThenBuilder then = then().withResult(result);
        PrimingRequestBuilder builder = PrimingRequest.queryBuilder().withQuery("mock query");

        if (config != null)
            then = then.withConfig(config);

        builder = builder.withThen(then);

        scassandras.node(hostNumber).primingClient().prime(builder.build());
    }

    protected void simulateNormalResponse(int hostNumber) {
        scassandras.node(hostNumber).primingClient().prime(PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withThen(then().withRows(row("result", "result1")))
                .build());
    }

    protected static List<Map<String, ?>> row(String key, String value) {
        return ImmutableList.<Map<String, ?>>of(ImmutableMap.of(key, value));
    }

    protected ResultSet query() {
        return query(session);
    }

    protected ResultSet queryWithCL(ConsistencyLevel cl) {
        Statement statement = new SimpleStatement("mock query").setConsistencyLevel(cl);
        return session.execute(statement);
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

    protected void assertOnRequestErrorWasCalled(int times, Class<? extends DriverException> expected) {
        Mockito.verify(retryPolicy, times(times)).onRequestError(
                any(Statement.class), any(ConsistencyLevel.class), any(expected), anyInt());
    }

    protected void assertQueried(int hostNumber, int times) {
        assertThat(scassandras.node(hostNumber).activityClient().retrieveQueries()).hasSize(times);
    }

    @AfterMethod(groups = "short", alwaysRun = true)
    public void afterMethod() {
        if (cluster != null)
            cluster.close();
        if (scassandras != null)
            scassandras.stop();
    }

    @DataProvider
    public static Object[][] serverSideErrors() {
        return new Object[][]{
                {server_error, ServerError.class},
                {overloaded, OverloadedException.class},
        };
    }

    @DataProvider
    public static Object[][] connectionErrors() {
        return new Object[][]{
                {CloseType.CLOSE},
                {CloseType.HALFCLOSE},
                {CloseType.RESET}
        };
    }
}
