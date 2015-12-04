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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;

import org.assertj.core.api.Fail;
import org.scassandra.Scassandra;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.Result.*;
import static org.scassandra.http.client.PrimingRequest.then;

import java.util.Collections;

public class DefaultRetryPolicyIntegrationTest extends AbstractRetryPolicyIntegrationTest {
    public DefaultRetryPolicyIntegrationTest() {
        super(DefaultRetryPolicy.INSTANCE);
    }

    @Test(groups = "short")
    public void should_rethrow_on_read_timeout_with_0_receivedResponses() {
        simulateError(1, read_request_timeout);

        try {
            query();
            fail("expected a ReadTimeoutException");
        } catch (ReadTimeoutException e) {/*expected*/ }

        assertOnReadTimeoutWasCalled(1);
        assertThat(errors.getReadTimeouts().getCount()).isEqualTo(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnReadTimeout().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_rethrow_on_write_timeout_with_SIMPLE_write_type() {
        simulateError(1, write_request_timeout);

        try {
            query();
            fail("expected a WriteTimeoutException");
        } catch (WriteTimeoutException e) {/*expected*/}

        assertOnWriteTimeoutWasCalled(1);
        assertThat(errors.getWriteTimeouts().getCount()).isEqualTo(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnWriteTimeout().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_try_next_host_on_first_unavailable() {
        simulateError(1, unavailable);
        simulateNormalResponse(2);

        query();

        assertOnUnavailableWasCalled(1);
        assertThat(errors.getUnavailables().getCount()).isEqualTo(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getRetriesOnUnavailable().getCount()).isEqualTo(1);
        assertQueried(1, 1);
        assertQueried(2, 1);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_rethrow_on_second_unavailable() {
        simulateError(1, unavailable);
        simulateError(2, unavailable);

        try {
            query();
            fail("expected an UnavailableException");
        } catch (UnavailableException e) {/*expected*/}

        assertOnUnavailableWasCalled(2);
        assertThat(errors.getUnavailables().getCount()).isEqualTo(2);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getRetriesOnUnavailable().getCount()).isEqualTo(1);
        assertQueried(1, 1);
        assertQueried(2, 1);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_rethrow_unavailable_in_no_host_available_exception() {
        LoadBalancingPolicy firstHostOnlyPolicy =
                new WhiteListPolicy(Policies.defaultLoadBalancingPolicy(),
                        Collections.singletonList(host1.getSocketAddress()));

        Cluster whiteListedCluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                // Scassandra does not support V3 nor V4 yet, and V4 may cause the server to crash
                .withProtocolVersion(ProtocolVersion.V2)
                .withRetryPolicy(retryPolicy)
                .withLoadBalancingPolicy(firstHostOnlyPolicy)
                .build();

        try {
            Session whiteListedSession = whiteListedCluster.connect();
            // Clear all activity as result of connect.
            for(Scassandra node : scassandras.nodes()) {
                node.activityClient().clearAllRecordedActivity();
            }

            simulateError(1, unavailable);

            try {
                query(whiteListedSession);
                fail("expected an NoHostAvailableException");
            } catch(NoHostAvailableException e) {
                // ok
                Throwable error = e.getErrors().get(host1.getSocketAddress());
                assertThat(error).isNotNull();
                assertThat(error).isInstanceOf(UnavailableException.class);
            }

            assertOnUnavailableWasCalled(1);
            // We expect a retry, but it was never sent because there were no more hosts.
            Metrics.Errors whiteListErrors = whiteListedCluster.getMetrics().getErrorMetrics();
            assertThat(whiteListErrors.getRetriesOnUnavailable().getCount()).isEqualTo(1);
            assertQueried(1, 1);
            assertQueried(2, 0);
            assertQueried(3, 0);
        } finally {
            whiteListedCluster.close();
        }
    }

    @Test(groups = "short")
    public void should_try_next_host_on_client_timeouts() {
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1);
        try {
            scassandras
                .node(1).primingClient().prime(PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withThen(then().withFixedDelay(1000L).withRows(row("result", "result1")))
                    .build());
            scassandras
                .node(2).primingClient().prime(PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withThen(then().withFixedDelay(1000L).withRows(row("result", "result2")))
                    .build());
            scassandras
                .node(3).primingClient().prime(PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withThen(then().withFixedDelay(1000L).withRows(row("result", "result3")))
                    .build());
            try {
                query();
                fail("expected a NoHostAvailableException");
            } catch (NoHostAvailableException e) {
                assertThat(e.getErrors().keySet()).hasSize(3).containsOnlyOnce(
                    host1.getSocketAddress(),
                    host2.getSocketAddress(),
                    host3.getSocketAddress());
                assertThat(e.getErrors().values())
                    .hasOnlyElementsOfType(OperationTimedOutException.class)
                    .extractingResultOf("getMessage")
                    .containsOnlyOnce(
                        String.format("[%s] Timed out waiting for server response", host1.getAddress()),
                        String.format("[%s] Timed out waiting for server response", host2.getAddress()),
                        String.format("[%s] Timed out waiting for server response", host3.getAddress())
                    );
            }
            assertOnRequestErrorWasCalled(3, OperationTimedOutException.class);
            assertThat(errors.getRetries().getCount()).isEqualTo(3);
            assertThat(errors.getClientTimeouts().getCount()).isEqualTo(3);
            assertThat(errors.getRetriesOnClientTimeout().getCount()).isEqualTo(3);
            assertQueried(1, 1);
            assertQueried(2, 1);
            assertQueried(3, 1);
        } finally {
            cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS);
        }
    }

    @DataProvider
    public static Object[][] serverSideErrors() {
        return new Object[][]{
            {server_error, ServerError.class},
            {overloaded, OverloadedException.class}
        };
    }

    @Test(groups = "short", dataProvider = "serverSideErrors")
    public void should_try_next_host_on_server_side_error(PrimingRequest.Result error, Class<? extends DriverException> exception) {
        simulateError(1, error);
        simulateError(2, error);
        simulateError(3, error);
        try {
            query();
            Fail.fail("expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertThat(e.getErrors().keySet()).hasSize(3).containsOnlyOnce(
                host1.getSocketAddress(),
                host2.getSocketAddress(),
                host3.getSocketAddress());
            assertThat(e.getErrors().values()).hasOnlyElementsOfType(exception);
        }
        assertOnRequestErrorWasCalled(3, exception);
        assertThat(errors.getOthers().getCount()).isEqualTo(3);
        assertThat(errors.getRetries().getCount()).isEqualTo(3);
        assertThat(errors.getRetriesOnOtherErrors().getCount()).isEqualTo(3);
        assertQueried(1, 1);
        assertQueried(2, 1);
        assertQueried(3, 1);
    }

}
