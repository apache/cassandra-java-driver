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

import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Fail.fail;
import static org.scassandra.http.client.PrimingRequest.Result.*;
import static org.scassandra.http.client.PrimingRequest.then;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;

import static com.datastax.driver.core.Assertions.assertThat;

/**
 * Integration test with a custom implementation, to test retry and ignore decisions.
 */
public class CustomRetryPolicyIntegrationTest extends AbstractRetryPolicyIntegrationTest {
    public CustomRetryPolicyIntegrationTest() {
        super(new CustomRetryPolicy());
    }

    @Test(groups = "short")
    public void should_ignore_read_timeout() {
        simulateError(1, read_request_timeout);

        ResultSet rs = query();
        assertThat(rs.iterator().hasNext()).isFalse(); // ignore decisions produce empty result sets

        assertOnReadTimeoutWasCalled(1);
        assertThat(errors.getIgnores().getCount()).isEqualTo(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getIgnoresOnReadTimeout().getCount()).isEqualTo(1);
        assertThat(errors.getRetriesOnReadTimeout().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_retry_once_on_same_host_on_unavailable() {
        simulateError(1, unavailable);

        try {
            query();
            fail("expected an UnavailableException");
        } catch (UnavailableException e) {/*expected*/}

        assertOnUnavailableWasCalled(2);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getUnavailables().getCount()).isEqualTo(2);
        assertThat(errors.getRetriesOnUnavailable().getCount()).isEqualTo(1);
        assertQueried(1, 2);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_rethrow_on_client_timeouts() {
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1);
        try {
            scassandras
                .node(1).primingClient().prime(PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withThen(then().withFixedDelay(1000L).withRows(row("result", "result1")))
                .build());
            try {
                query();
                fail("expected a DriverException");
            } catch (DriverException e) {
                assertThat(e.getMessage()).isEqualTo(
                    String.format("[%s]: Timed out waiting for server response", host1.getSocketAddress())
                );
            }
            assertOnRequestErrorWasCalled(1);
            assertThat(errors.getRetries().getCount()).isEqualTo(0);
            assertThat(errors.getClientTimeouts().getCount()).isEqualTo(1);
            assertThat(errors.getRetriesOnClientTimeout().getCount()).isEqualTo(0);
            assertQueried(1, 1);
            assertQueried(2, 0);
            assertQueried(3, 0);
        } finally {
            cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS);
        }
    }

    @DataProvider
    public static Object[][] serverSideErrors() {
        return new Object[][]{
            {server_error, ServerError.class},
            {overloaded, OverloadedException.class},
        };
    }

    @Test(groups = "short", dataProvider = "serverSideErrors")
    public void should_rethrow_on_server_side_error(PrimingRequest.Result error, Class<? extends DriverException> exception) {
        simulateError(1, error);
        try {
            query();
            fail("expected a DriverException");
        } catch (DriverException e) {
            assertThat(e).isInstanceOf(exception);
        }
        assertOnRequestErrorWasCalled(1);
        assertThat(errors.getOthers().getCount()).isEqualTo(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnOtherErrors().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    /**
     * Ignores read and write timeouts, and retries at most once on unavailable.
     * Rethrows for unexpected errors.
     */
    static class CustomRetryPolicy implements ExtendedRetryPolicy {

        @Override
        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            return RetryDecision.ignore();
        }

        @Override
        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            return RetryDecision.ignore();
        }

        @Override
        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            return (nbRetry == 0)
                ? RetryDecision.retry(cl)
                : RetryDecision.rethrow();
        }

        @Override
        public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, Exception e, int nbRetry) {
            return RetryDecision.rethrow();
        }
    }
}
