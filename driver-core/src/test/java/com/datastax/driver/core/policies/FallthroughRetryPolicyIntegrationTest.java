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

import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.*;
import org.assertj.core.api.Fail;
import org.scassandra.http.client.ClosedConnectionConfig;
import org.scassandra.http.client.ClosedConnectionConfig.CloseType;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Result;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.*;

public class FallthroughRetryPolicyIntegrationTest extends AbstractRetryPolicyIntegrationTest {
    public FallthroughRetryPolicyIntegrationTest() {
        super(FallthroughRetryPolicy.INSTANCE);
    }

    @Test(groups = "short")
    public void should_rethrow_on_read_timeout_with_0_receivedResponses() {
        simulateError(1, read_request_timeout);

        try {
            query();
            fail("expected a ReadTimeoutException");
        } catch (ReadTimeoutException e) {/*expected*/ }

        assertOnReadTimeoutWasCalled(1);
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
        assertThat(errors.getRetriesOnWriteTimeout().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_rethrow_on_unavailable() {
        simulateError(1, unavailable);

        try {
            query();
            fail("expected an UnavailableException");
        } catch (UnavailableException e) {/*expected*/}

        assertOnUnavailableWasCalled(1);
        assertThat(errors.getRetriesOnUnavailable().getCount()).isEqualTo(0);
        assertQueried(1, 1);
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
                Fail.fail("expected an OperationTimedOutException");
            } catch (OperationTimedOutException e) {
                assertThat(e.getMessage()).isEqualTo(
                        String.format("[%s] Timed out waiting for server response", host1.getSocketAddress())
                );
            }
            assertOnRequestErrorWasCalled(1, OperationTimedOutException.class);
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


    @Test(groups = "short", dataProvider = "serverSideErrors")
    public void should_rethrow_on_server_side_error(Result error, Class<? extends DriverException> exception) {
        simulateError(1, error);
        try {
            query();
            Fail.fail("expected a DriverException");
        } catch (DriverException e) {
            assertThat(e).isInstanceOf(exception);
        }
        assertOnRequestErrorWasCalled(1, ServerError.class);
        assertThat(errors.getOthers().getCount()).isEqualTo(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnOtherErrors().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }


    @Test(groups = "short", dataProvider = "connectionErrors")
    public void should_rethrow_on_connection_error(CloseType closeType) {
        simulateError(1, Result.closed_connection, new ClosedConnectionConfig(closeType));
        try {
            query();
            Fail.fail("expected a TransportException");
        } catch (TransportException e) {
            assertThat(e.getMessage()).isEqualTo(
                    String.format("[%s] Connection has been closed", host1.getSocketAddress())
            );
        }
        assertOnRequestErrorWasCalled(1, TransportException.class);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getConnectionErrors().getCount()).isEqualTo(1);
        assertThat(errors.getIgnoresOnConnectionError().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnConnectionError().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }
}
