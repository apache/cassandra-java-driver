/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.internal.core.cql;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.DefaultWriteType;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.error.ReadTimeout;
import com.datastax.oss.protocol.internal.response.error.Unavailable;
import com.datastax.oss.protocol.internal.response.error.WriteTimeout;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class CqlRequestHandlerRetryTest extends CqlRequestHandlerTestBase {

  @Test
  @UseDataProvider("allIdempotenceConfigs")
  public void should_always_try_next_node_if_bootstrapping(
      boolean defaultIdempotence, Statement<?> statement) {
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            .withDefaultIdempotence(defaultIdempotence)
            .withResponse(
                node1,
                defaultFrameOf(
                    new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")))
            .withResponse(node2, defaultFrameOf(singleRow()))
            .build()) {

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(
              resultSet -> {
                Iterator<Row> rows = resultSet.currentPage().iterator();
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getString("message")).isEqualTo("hello, world");

                ExecutionInfo executionInfo = resultSet.getExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node2);
                assertThat(executionInfo.getErrors()).hasSize(1);
                assertThat(executionInfo.getErrors().get(0).getKey()).isEqualTo(node1);
                assertThat(executionInfo.getErrors().get(0).getValue())
                    .isInstanceOf(BootstrappingException.class);
                assertThat(executionInfo.getIncomingPayload()).isEmpty();
                assertThat(executionInfo.getPagingState()).isNull();
                assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
                assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
                assertThat(executionInfo.getWarnings()).isEmpty();

                verifyNoMoreInteractions(harness.getContext().getRetryPolicy(anyString()));
              });
    }
  }

  @Test
  @UseDataProvider("allIdempotenceConfigs")
  public void should_always_rethrow_query_validation_error(
      boolean defaultIdempotence, Statement<?> statement) {
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            .withDefaultIdempotence(defaultIdempotence)
            .withResponse(
                node1,
                defaultFrameOf(new Error(ProtocolConstants.ErrorCode.INVALID, "mock message")))
            .build()) {

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isFailed(
              error -> {
                assertThat(error)
                    .isInstanceOf(InvalidQueryException.class)
                    .hasMessage("mock message");
                verifyNoMoreInteractions(harness.getContext().getRetryPolicy(anyString()));

                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        DefaultNodeMetric.OTHER_ERRORS, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(1))
                    .isEnabled(DefaultNodeMetric.CQL_MESSAGES, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1)
                    .updateTimer(
                        eq(DefaultNodeMetric.CQL_MESSAGES),
                        eq(DriverExecutionProfile.DEFAULT_NAME),
                        anyLong(),
                        eq(TimeUnit.NANOSECONDS));
                verifyNoMoreInteractions(nodeMetricUpdater1);
              });
    }
  }

  @Test
  @UseDataProvider("failureAndIdempotent")
  public void should_try_next_node_if_idempotent_and_retry_policy_decides_so(
      FailureScenario failureScenario, boolean defaultIdempotence, Statement<?> statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    failureScenario.mockRequestError(harnessBuilder, node1);
    harnessBuilder.withResponse(node2, defaultFrameOf(singleRow()));

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      failureScenario.mockRetryPolicyVerdict(
          harness.getContext().getRetryPolicy(anyString()), RetryVerdict.RETRY_NEXT);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(
              resultSet -> {
                Iterator<Row> rows = resultSet.currentPage().iterator();
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getString("message")).isEqualTo("hello, world");

                ExecutionInfo executionInfo = resultSet.getExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node2);
                assertThat(executionInfo.getErrors()).hasSize(1);
                assertThat(executionInfo.getErrors().get(0).getKey()).isEqualTo(node1);

                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        failureScenario.errorMetric, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        DefaultNodeMetric.RETRIES, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        failureScenario.retryMetric, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(1))
                    .isEnabled(DefaultNodeMetric.CQL_MESSAGES, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(1))
                    .updateTimer(
                        eq(DefaultNodeMetric.CQL_MESSAGES),
                        eq(DriverExecutionProfile.DEFAULT_NAME),
                        anyLong(),
                        eq(TimeUnit.NANOSECONDS));
                verifyNoMoreInteractions(nodeMetricUpdater1);
              });
    }
  }

  @Test
  @UseDataProvider("failureAndIdempotent")
  public void should_try_same_node_if_idempotent_and_retry_policy_decides_so(
      FailureScenario failureScenario, boolean defaultIdempotence, Statement<?> statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    failureScenario.mockRequestError(harnessBuilder, node1);
    harnessBuilder.withResponse(node1, defaultFrameOf(singleRow()));

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      failureScenario.mockRetryPolicyVerdict(
          harness.getContext().getRetryPolicy(anyString()), RetryVerdict.RETRY_SAME);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(
              resultSet -> {
                Iterator<Row> rows = resultSet.currentPage().iterator();
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getString("message")).isEqualTo("hello, world");

                ExecutionInfo executionInfo = resultSet.getExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node1);
                assertThat(executionInfo.getErrors()).hasSize(1);
                assertThat(executionInfo.getErrors().get(0).getKey()).isEqualTo(node1);

                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        failureScenario.errorMetric, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        DefaultNodeMetric.RETRIES, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        failureScenario.retryMetric, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(2))
                    .isEnabled(DefaultNodeMetric.CQL_MESSAGES, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(2))
                    .updateTimer(
                        eq(DefaultNodeMetric.CQL_MESSAGES),
                        eq(DriverExecutionProfile.DEFAULT_NAME),
                        anyLong(),
                        eq(TimeUnit.NANOSECONDS));
                verifyNoMoreInteractions(nodeMetricUpdater1);
              });
    }
  }

  @Test
  @UseDataProvider("failureAndIdempotent")
  public void should_ignore_error_if_idempotent_and_retry_policy_decides_so(
      FailureScenario failureScenario, boolean defaultIdempotence, Statement<?> statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    failureScenario.mockRequestError(harnessBuilder, node1);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      failureScenario.mockRetryPolicyVerdict(
          harness.getContext().getRetryPolicy(anyString()), RetryVerdict.IGNORE);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(
              resultSet -> {
                Iterator<Row> rows = resultSet.currentPage().iterator();
                assertThat(rows.hasNext()).isFalse();

                ExecutionInfo executionInfo = resultSet.getExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node1);
                assertThat(executionInfo.getErrors()).hasSize(0);

                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        failureScenario.errorMetric, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        DefaultNodeMetric.IGNORES, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        failureScenario.ignoreMetric, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(1))
                    .isEnabled(DefaultNodeMetric.CQL_MESSAGES, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(1))
                    .updateTimer(
                        eq(DefaultNodeMetric.CQL_MESSAGES),
                        eq(DriverExecutionProfile.DEFAULT_NAME),
                        anyLong(),
                        eq(TimeUnit.NANOSECONDS));
                verifyNoMoreInteractions(nodeMetricUpdater1);
              });
    }
  }

  @Test
  @UseDataProvider("failureAndIdempotent")
  public void should_rethrow_error_if_idempotent_and_retry_policy_decides_so(
      FailureScenario failureScenario, boolean defaultIdempotence, Statement<?> statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    failureScenario.mockRequestError(harnessBuilder, node1);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      failureScenario.mockRetryPolicyVerdict(
          harness.getContext().getRetryPolicy(anyString()), RetryVerdict.RETHROW);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isFailed(
              error -> {
                assertThat(error).isInstanceOf(failureScenario.expectedExceptionClass);

                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        failureScenario.errorMetric, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(1))
                    .isEnabled(DefaultNodeMetric.CQL_MESSAGES, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(1))
                    .updateTimer(
                        eq(DefaultNodeMetric.CQL_MESSAGES),
                        eq(DriverExecutionProfile.DEFAULT_NAME),
                        anyLong(),
                        eq(TimeUnit.NANOSECONDS));
                verifyNoMoreInteractions(nodeMetricUpdater1);
              });
    }
  }

  @Test
  @UseDataProvider("failureAndNotIdempotent")
  public void should_rethrow_error_if_not_idempotent_and_error_unsafe_or_policy_rethrows(
      FailureScenario failureScenario, boolean defaultIdempotence, Statement<?> statement) {

    // For two of the possible exceptions, the retry policy is called even if the statement is not
    // idempotent
    boolean shouldCallRetryPolicy =
        (failureScenario.expectedExceptionClass.equals(UnavailableException.class)
            || failureScenario.expectedExceptionClass.equals(ReadTimeoutException.class));

    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    failureScenario.mockRequestError(harnessBuilder, node1);
    harnessBuilder.withResponse(node2, defaultFrameOf(singleRow()));

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      if (shouldCallRetryPolicy) {
        failureScenario.mockRetryPolicyVerdict(
            harness.getContext().getRetryPolicy(anyString()), RetryVerdict.RETHROW);
      }

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isFailed(
              error -> {
                assertThat(error).isInstanceOf(failureScenario.expectedExceptionClass);
                // When non idempotent, the policy is bypassed completely:
                if (!shouldCallRetryPolicy) {
                  verifyNoMoreInteractions(harness.getContext().getRetryPolicy(anyString()));
                }

                verify(nodeMetricUpdater1)
                    .incrementCounter(
                        failureScenario.errorMetric, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(1))
                    .isEnabled(DefaultNodeMetric.CQL_MESSAGES, DriverExecutionProfile.DEFAULT_NAME);
                verify(nodeMetricUpdater1, atMost(1))
                    .updateTimer(
                        eq(DefaultNodeMetric.CQL_MESSAGES),
                        eq(DriverExecutionProfile.DEFAULT_NAME),
                        anyLong(),
                        eq(TimeUnit.NANOSECONDS));
                verifyNoMoreInteractions(nodeMetricUpdater1);
              });
    }
  }

  /**
   * Sets up the mocks to simulate an error from a node, and make the retry policy return a given
   * decision for that error.
   */
  private abstract static class FailureScenario {
    private final Class<? extends Throwable> expectedExceptionClass;
    final DefaultNodeMetric errorMetric;
    final DefaultNodeMetric retryMetric;
    final DefaultNodeMetric ignoreMetric;

    protected FailureScenario(
        Class<? extends Throwable> expectedExceptionClass,
        DefaultNodeMetric errorMetric,
        DefaultNodeMetric retryMetric,
        DefaultNodeMetric ignoreMetric) {
      this.expectedExceptionClass = expectedExceptionClass;
      this.errorMetric = errorMetric;
      this.retryMetric = retryMetric;
      this.ignoreMetric = ignoreMetric;
    }

    abstract void mockRequestError(RequestHandlerTestHarness.Builder builder, Node node);

    abstract void mockRetryPolicyVerdict(RetryPolicy policy, RetryVerdict verdict);
  }

  @DataProvider
  public static Object[][] failure() {
    return TestDataProviders.fromList(
        new FailureScenario(
            ReadTimeoutException.class,
            DefaultNodeMetric.READ_TIMEOUTS,
            DefaultNodeMetric.RETRIES_ON_READ_TIMEOUT,
            DefaultNodeMetric.IGNORES_ON_READ_TIMEOUT) {
          @Override
          public void mockRequestError(RequestHandlerTestHarness.Builder builder, Node node) {
            builder.withResponse(
                node,
                defaultFrameOf(
                    new ReadTimeout(
                        "mock message", ProtocolConstants.ConsistencyLevel.LOCAL_ONE, 1, 2, true)));
          }

          @Override
          public void mockRetryPolicyVerdict(RetryPolicy policy, RetryVerdict verdict) {
            when(policy.onReadTimeoutVerdict(
                    any(Statement.class),
                    eq(DefaultConsistencyLevel.LOCAL_ONE),
                    eq(2),
                    eq(1),
                    eq(true),
                    eq(0)))
                .thenReturn(verdict);
          }
        },
        new FailureScenario(
            WriteTimeoutException.class,
            DefaultNodeMetric.WRITE_TIMEOUTS,
            DefaultNodeMetric.RETRIES_ON_WRITE_TIMEOUT,
            DefaultNodeMetric.IGNORES_ON_WRITE_TIMEOUT) {
          @Override
          public void mockRequestError(RequestHandlerTestHarness.Builder builder, Node node) {
            builder.withResponse(
                node,
                defaultFrameOf(
                    new WriteTimeout(
                        "mock message",
                        ProtocolConstants.ConsistencyLevel.LOCAL_ONE,
                        1,
                        2,
                        ProtocolConstants.WriteType.SIMPLE)));
          }

          @Override
          public void mockRetryPolicyVerdict(RetryPolicy policy, RetryVerdict verdict) {
            when(policy.onWriteTimeoutVerdict(
                    any(Statement.class),
                    eq(DefaultConsistencyLevel.LOCAL_ONE),
                    eq(DefaultWriteType.SIMPLE),
                    eq(2),
                    eq(1),
                    eq(0)))
                .thenReturn(verdict);
          }
        },
        new FailureScenario(
            UnavailableException.class,
            DefaultNodeMetric.UNAVAILABLES,
            DefaultNodeMetric.RETRIES_ON_UNAVAILABLE,
            DefaultNodeMetric.IGNORES_ON_UNAVAILABLE) {
          @Override
          public void mockRequestError(RequestHandlerTestHarness.Builder builder, Node node) {
            builder.withResponse(
                node,
                defaultFrameOf(
                    new Unavailable(
                        "mock message", ProtocolConstants.ConsistencyLevel.LOCAL_ONE, 2, 1)));
          }

          @Override
          public void mockRetryPolicyVerdict(RetryPolicy policy, RetryVerdict verdict) {
            when(policy.onUnavailableVerdict(
                    any(Statement.class),
                    eq(DefaultConsistencyLevel.LOCAL_ONE),
                    eq(2),
                    eq(1),
                    eq(0)))
                .thenReturn(verdict);
          }
        },
        new FailureScenario(
            ServerError.class,
            DefaultNodeMetric.OTHER_ERRORS,
            DefaultNodeMetric.RETRIES_ON_OTHER_ERROR,
            DefaultNodeMetric.IGNORES_ON_OTHER_ERROR) {
          @Override
          public void mockRequestError(RequestHandlerTestHarness.Builder builder, Node node) {
            builder.withResponse(
                node,
                defaultFrameOf(
                    new Error(ProtocolConstants.ErrorCode.SERVER_ERROR, "mock server error")));
          }

          @Override
          public void mockRetryPolicyVerdict(RetryPolicy policy, RetryVerdict verdict) {
            when(policy.onErrorResponseVerdict(any(Statement.class), any(ServerError.class), eq(0)))
                .thenReturn(verdict);
          }
        },
        new FailureScenario(
            HeartbeatException.class,
            DefaultNodeMetric.ABORTED_REQUESTS,
            DefaultNodeMetric.RETRIES_ON_ABORTED,
            DefaultNodeMetric.IGNORES_ON_ABORTED) {
          @Override
          public void mockRequestError(RequestHandlerTestHarness.Builder builder, Node node) {
            builder.withResponseFailure(node, mock(HeartbeatException.class));
          }

          @Override
          public void mockRetryPolicyVerdict(RetryPolicy policy, RetryVerdict verdict) {
            when(policy.onRequestAbortedVerdict(
                    any(Statement.class), any(HeartbeatException.class), eq(0)))
                .thenReturn(verdict);
          }
        });
  }

  @DataProvider
  public static Object[][] failureAndIdempotent() {
    return TestDataProviders.combine(failure(), idempotentConfig());
  }

  @DataProvider
  public static Object[][] failureAndNotIdempotent() {
    return TestDataProviders.combine(failure(), nonIdempotentConfig());
  }
}
