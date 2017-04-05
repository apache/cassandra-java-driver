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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.*;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Fail;
import org.scassandra.http.client.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.scassandra.http.client.Consistency.SERIAL;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.*;
import static org.scassandra.http.client.WriteTypePrime.UNLOGGED_BATCH;
import static org.testng.Assert.fail;

public class DowngradingConsistencyRetryPolicyIntegrationTest extends AbstractRetryPolicyIntegrationTest {

    public DowngradingConsistencyRetryPolicyIntegrationTest() {
        super(DowngradingConsistencyRetryPolicy.INSTANCE);
    }

    /**
     * @return An array of pairs that match # of alive replicas with the expected downgraded CL used on read/write/unavailable.
     */
    @DataProvider
    public static Object[][] consistencyLevels() {
        return new Object[][]{
                {4, ConsistencyLevel.THREE},
                {3, ConsistencyLevel.THREE},
                {2, ConsistencyLevel.TWO},
                {1, ConsistencyLevel.ONE}
        };
    }


    /**
     * @return Write Types for which we expect a rethrow if used and there are no received acks.
     */
    @DataProvider
    public static Object[][] rethrowWriteTypes() {
        return new Object[][]{
                {WriteTypePrime.SIMPLE},
                {WriteTypePrime.BATCH},
                {WriteTypePrime.COUNTER},
                {WriteTypePrime.CAS}
        };
    }


    /**
     * @return Write Types for which we expect an ignore if used and there are received acks.
     */
    @DataProvider
    public static Object[][] ignoreWriteTypesWithReceivedAcks() {
        return new Object[][]{
                {WriteTypePrime.SIMPLE},
                {WriteTypePrime.BATCH}
        };
    }

    /**
     * Ensures that when handling a read timeout with {@link DowngradingConsistencyRetryPolicy} that a retry is
     * reattempted with {@link ConsistencyLevel#ONE} if the consistency level on the statement executed is
     * {@link ConsistencyLevel#EACH_QUORUM}, even if the number of known alive replicas was 0.
     *
     * @jira_ticket JAVA-1005
     * @test_category retry_policy
     */
    @Test(groups = "short")
    public void should_retry_once_on_same_host_from_each_quorum_to_one() {
        simulateError(1, read_request_timeout, new ReadTimeoutConfig(0, 3, false));

        try {
            queryWithCL(ConsistencyLevel.EACH_QUORUM);
        } catch (ReadTimeoutException e) {
            assertThat(e.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ONE);
        }

        assertOnReadTimeoutWasCalled(2);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getReadTimeouts().getCount()).isEqualTo(2);
        assertThat(errors.getRetriesOnReadTimeout().getCount()).isEqualTo(1);
        assertQueried(1, 2);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    /**
     * Ensures that when handling a read timeout with {@link DowngradingConsistencyRetryPolicy} that a retry is
     * reattempted with a {@link ConsistencyLevel} that matches min(received acknowledgements, THREE) and is only
     * retried once.
     *
     * @param received             The number of received acknowledgements to use in read timeout.
     * @param expectedDowngradedCL The consistency level that is expected to be used on the retry.
     * @test_category retry_policy
     */
    @Test(groups = "short", dataProvider = "consistencyLevels")
    public void should_retry_once_on_same_host_with_reduced_consistency_level_on_read_timeout(int received, ConsistencyLevel expectedDowngradedCL) {
        simulateError(1, read_request_timeout, new ReadTimeoutConfig(received, received + 1, true));

        try {
            query();
            fail("expected an ReadTimeoutException");
        } catch (ReadTimeoutException e) {
            assertThat(e.getConsistencyLevel()).isEqualTo(expectedDowngradedCL);
        }

        assertOnReadTimeoutWasCalled(2);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getReadTimeouts().getCount()).isEqualTo(2);
        assertThat(errors.getRetriesOnReadTimeout().getCount()).isEqualTo(1);
        assertQueried(1, 2);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }


    /**
     * Ensures that when handling a read timeout with {@link DowngradingConsistencyRetryPolicy} that a retry is
     * reattempted if data was not retrieved, but enough replicas were alive to handle the request.
     *
     * @test_category retry_policy
     */
    @Test(groups = "short")
    public void should_retry_once_if_not_data_was_retrieved_and_enough_replicas_alive() {
        simulateError(1, read_request_timeout, new ReadTimeoutConfig(1, 1, false));

        try {
            query();
            fail("expected an ReadTimeoutException");
        } catch (ReadTimeoutException e) {/*expected*/}

        assertOnReadTimeoutWasCalled(2);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getReadTimeouts().getCount()).isEqualTo(2);
        assertThat(errors.getRetriesOnReadTimeout().getCount()).isEqualTo(1);
        assertQueried(1, 2);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }


    /**
     * Ensures that when handling a read timeout with {@link DowngradingConsistencyRetryPolicy} that a retry is not
     * attempted if no replicas were alive.   In a real scenario, this would not be expected as we'd anticipate an
     * {@link UnavailableException} instead.
     *
     * @test_category retry_policy
     */
    @Test(groups = "short")
    public void should_rethrow_if_no_hosts_alive_on_read_timeout() {
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


    /**
     * Ensures that when handling a write timeout with {@link DowngradingConsistencyRetryPolicy} that it rethrows
     * the exception if the {@link WriteType} was any of {@link #rethrowWriteTypes}.
     *
     * @param writeType writeType communicated by {@link WriteTimeoutException}.
     * @test_category retry_policy
     */
    @Test(groups = "short", dataProvider = "rethrowWriteTypes")
    public void should_rethrow_on_write_timeout_with_write_type(WriteTypePrime writeType) {
        simulateError(1, write_request_timeout, new WriteTimeoutConfig(writeType, 0, 2));

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


    /**
     * Ensures that when handling a write timeout with {@link DowngradingConsistencyRetryPolicy} that it ignores
     * the exception if the {@link WriteType} was any of {@link #ignoreWriteTypesWithReceivedAcks} and we received acks
     * some at least one replica.
     *
     * @param writeType writeType communicated by {@link WriteTimeoutException}.
     * @test_category retry_policy
     */
    @Test(groups = "short", dataProvider = "ignoreWriteTypesWithReceivedAcks")
    public void should_ignore_on_write_timeout_with_write_type_and_received_acks(WriteTypePrime writeType) {
        simulateError(1, write_request_timeout, new WriteTimeoutConfig(writeType, 1, 2));

        query();

        assertOnWriteTimeoutWasCalled(1);
        assertThat(errors.getWriteTimeouts().getCount()).isEqualTo(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnWriteTimeout().getCount()).isEqualTo(0);
        assertThat(errors.getIgnoresOnWriteTimeout().getCount()).isEqualTo(1);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }


    /**
     * Ensures that when handling a write timeout with {@link DowngradingConsistencyRetryPolicy} that a retry is
     * attempted on the same host if the {@link WriteType} is {@link WriteType#BATCH_LOG}.
     *
     * @test_category retry_policy
     */
    @Test(groups = "short")
    public void should_retry_once_on_same_host_with_BATCH_LOG_write_type() {
        simulateError(1, write_request_timeout, new WriteTimeoutConfig(WriteTypePrime.BATCH_LOG, 1, 2));

        try {
            query();
            fail("expected a WriteTimeoutException");
        } catch (WriteTimeoutException e) {/*expected*/}

        assertOnWriteTimeoutWasCalled(2);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getWriteTimeouts().getCount()).isEqualTo(2);
        assertThat(errors.getRetriesOnWriteTimeout().getCount()).isEqualTo(1);
        assertQueried(1, 2);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }


    /**
     * Ensures that when handling a write timeout with {@link DowngradingConsistencyRetryPolicy} that a retry is
     * attempted on the same host with a reduced consistency level that matches min(received acknowledgments, THREE)
     * if the {@link WriteType} is {@link WriteType#UNLOGGED_BATCH} and is only retries once.
     *
     * @param alive                The number of received acknowledgements to use in write timeout.
     * @param expectedDowngradedCL The consistency level that is expected to be used on the retry.
     * @test_category retry_policy
     */
    @Test(groups = "short", dataProvider = "consistencyLevels")
    public void should_retry_once_on_same_host_with_reduced_consistency_level_on_write_timeout(int alive, ConsistencyLevel expectedDowngradedCL) {
        simulateError(1, write_request_timeout, new WriteTimeoutConfig(UNLOGGED_BATCH, alive, alive + 1));

        try {
            query();
            fail("expected a WriteTimeoutException");
        } catch (WriteTimeoutException e) {
            assertThat(e.getConsistencyLevel()).isEqualTo(expectedDowngradedCL);
        }

        assertOnWriteTimeoutWasCalled(2);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getWriteTimeouts().getCount()).isEqualTo(2);
        assertThat(errors.getRetriesOnWriteTimeout().getCount()).isEqualTo(1);
        assertQueried(1, 2);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }


    /**
     * Ensures that when handling an unavailable with {@link DowngradingConsistencyRetryPolicy} that a retry is
     * reattempted with a {@link ConsistencyLevel} that matches min(received acknowledgements, THREE) and is only
     * retried once.
     *
     * @param alive                The number of received acknowledgements to use in unavailable.
     * @param expectedDowngradedCL The consistency level that is expected to be used on the retry.
     * @test_category retry_policy
     */
    @Test(groups = "short", dataProvider = "consistencyLevels")
    public void should_retry_once_on_same_host_with_reduced_consistency_level_on_unavailable(int alive, ConsistencyLevel expectedDowngradedCL) {
        simulateError(1, unavailable, new UnavailableConfig(alive + 1, alive));

        try {
            query();
            fail("expected an UnavailableException");
        } catch (UnavailableException e) {
            assertThat(e.getConsistencyLevel()).isEqualTo(expectedDowngradedCL);
        }

        assertOnUnavailableWasCalled(2);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getUnavailables().getCount()).isEqualTo(2);
        assertThat(errors.getRetriesOnUnavailable().getCount()).isEqualTo(1);
        assertQueried(1, 2);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }


    /**
     * Ensures that when handling an unavailable with {@link DowngradingConsistencyRetryPolicy} that a retry is
     * is not reattempted if no replicas are alive.
     *
     * @test_category retry_policy
     */
    @Test(groups = "short")
    public void should_rethrow_if_no_hosts_alive_on_unavailable() {
        simulateError(1, unavailable, new UnavailableConfig(1, 0));

        try {
            query();
            fail("expected an UnavailableException");
        } catch (UnavailableException e) {/*expected*/}

        assertOnUnavailableWasCalled(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getUnavailables().getCount()).isEqualTo(1);
        assertThat(errors.getRetriesOnUnavailable().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    /**
     * Ensures that when handling a client timeout with {@link DowngradingConsistencyRetryPolicy} that a retry is
     * attempted on the next host until all hosts are tried at which point a {@link NoHostAvailableException} is
     * returned.
     *
     * @test_category retry_policy
     */
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
                Assertions.fail("expected a NoHostAvailableException");
            } catch (NoHostAvailableException e) {
                assertThat(e.getErrors().keySet()).hasSize(3).containsOnly(
                        host1.getSocketAddress(),
                        host2.getSocketAddress(),
                        host3.getSocketAddress());
                assertThat(e.getErrors().values())
                        .hasOnlyElementsOfType(OperationTimedOutException.class)
                        .extractingResultOf("getMessage")
                        .containsOnlyOnce(
                                String.format("[%s] Timed out waiting for server response", host1.getSocketAddress()),
                                String.format("[%s] Timed out waiting for server response", host2.getSocketAddress()),
                                String.format("[%s] Timed out waiting for server response", host3.getSocketAddress())
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


    /**
     * Ensures that when handling a server error defined in {@link #serverSideErrors} with
     * {@link DowngradingConsistencyRetryPolicy} that a retry is attempted on the next host until all hosts are tried
     * at which point a {@link NoHostAvailableException} is raised and it's errors include the expected exception.
     *
     * @param error     Server side error to be produced.
     * @param exception The exception we expect to be raised.
     * @test_category retry_policy
     */
    @Test(groups = "short", dataProvider = "serverSideErrors")
    public void should_try_next_host_on_server_side_error(Result error, Class<? extends DriverException> exception) {
        simulateError(1, error);
        simulateError(2, error);
        simulateError(3, error);
        try {
            query();
            Fail.fail("expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertThat(e.getErrors().keySet()).hasSize(3).containsOnly(
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


    /**
     * Ensures that when handling a connection error caused by the connection closing during a request in a way
     * described by {@link #connectionErrors} that the next host is tried.
     *
     * @param closeType The way the connection should be closed during the request.
     */
    @Test(groups = "short", dataProvider = "connectionErrors")
    public void should_try_next_host_on_connection_error(ClosedConnectionConfig.CloseType closeType) {
        simulateError(1, closed_connection, new ClosedConnectionConfig(closeType));
        simulateError(2, closed_connection, new ClosedConnectionConfig(closeType));
        simulateError(3, closed_connection, new ClosedConnectionConfig(closeType));
        try {
            query();
            Fail.fail("expected a TransportException");
        } catch (NoHostAvailableException e) {
            assertThat(e.getErrors().keySet()).hasSize(3).containsOnly(
                    host1.getSocketAddress(),
                    host2.getSocketAddress(),
                    host3.getSocketAddress());
            assertThat(e.getErrors().values()).hasOnlyElementsOfType(TransportException.class);
        }
        assertOnRequestErrorWasCalled(3, TransportException.class);
        assertThat(errors.getRetries().getCount()).isEqualTo(3);
        assertThat(errors.getConnectionErrors().getCount()).isEqualTo(3);
        assertThat(errors.getIgnoresOnConnectionError().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnConnectionError().getCount()).isEqualTo(3);
        assertQueried(1, 1);
        assertQueried(2, 1);
        assertQueried(3, 1);
    }

    @Test(groups = "short")
    public void should_rethrow_on_unavailable_if_CAS() {
        simulateError(1, unavailable, new UnavailableConfig(1, 0, SERIAL));
        simulateError(2, unavailable, new UnavailableConfig(1, 0, SERIAL));

        try {
            query();
            fail("expected an UnavailableException");
        } catch (UnavailableException e) {
            assertThat(e.getConsistencyLevel()).isEqualTo(ConsistencyLevel.SERIAL);
        }

        assertOnUnavailableWasCalled(2);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getUnavailables().getCount()).isEqualTo(2);
        assertThat(errors.getRetriesOnUnavailable().getCount()).isEqualTo(1);
        assertQueried(1, 1);
        assertQueried(2, 1);
        assertQueried(3, 0);
    }
}
