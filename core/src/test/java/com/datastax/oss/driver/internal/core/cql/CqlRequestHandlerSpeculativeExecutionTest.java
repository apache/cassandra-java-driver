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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.util.concurrent.ScheduledTaskCapturingEventLoop;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.Mockito;

public class CqlRequestHandlerSpeculativeExecutionTest extends CqlRequestHandlerTestBase {

  @Test
  @UseDataProvider("nonIdempotentConfig")
  public void should_not_schedule_speculative_executions_if_not_idempotent(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy(DriverConfigProfile.DEFAULT_NAME);

      new CqlRequestAsyncHandler(statement, harness.getSession(), harness.getContext(), "test")
          .handle();

      node1Behavior.verifyWrite();

      harness.nextScheduledTask(); // Discard the timeout task
      assertThat(harness.nextScheduledTask()).isNull();

      Mockito.verifyNoMoreInteractions(speculativeExecutionPolicy);
      Mockito.verifyNoMoreInteractions(nodeMetricUpdater1);
    }
  }

  @Test
  @UseDataProvider("idempotentConfig")
  public void should_schedule_speculative_executions(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy(DriverConfigProfile.DEFAULT_NAME);
      long firstExecutionDelay = 100L;
      long secondExecutionDelay = 200L;
      Mockito.when(
              speculativeExecutionPolicy.nextExecution(
                  any(Node.class), eq(null), eq(statement), eq(1)))
          .thenReturn(firstExecutionDelay);
      Mockito.when(
              speculativeExecutionPolicy.nextExecution(
                  any(Node.class), eq(null), eq(statement), eq(2)))
          .thenReturn(secondExecutionDelay);
      Mockito.when(
              speculativeExecutionPolicy.nextExecution(
                  any(Node.class), eq(null), eq(statement), eq(3)))
          .thenReturn(-1L);

      new CqlRequestAsyncHandler(statement, harness.getSession(), harness.getContext(), "test")
          .handle();

      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();

      harness.nextScheduledTask(); // Discard the timeout task

      ScheduledTaskCapturingEventLoop.CapturedTask<?> firstExecutionTask =
          harness.nextScheduledTask();
      assertThat(firstExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(firstExecutionDelay);
      Mockito.verifyNoMoreInteractions(nodeMetricUpdater1);
      firstExecutionTask.run();
      Mockito.verify(nodeMetricUpdater1)
          .incrementCounter(
              DefaultNodeMetric.SPECULATIVE_EXECUTIONS, DriverConfigProfile.DEFAULT_NAME);
      node2Behavior.verifyWrite();
      node2Behavior.setWriteSuccess();

      ScheduledTaskCapturingEventLoop.CapturedTask<?> secondExecutionTask =
          harness.nextScheduledTask();
      assertThat(secondExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(secondExecutionDelay);
      Mockito.verifyNoMoreInteractions(nodeMetricUpdater2);
      secondExecutionTask.run();
      Mockito.verify(nodeMetricUpdater2)
          .incrementCounter(
              DefaultNodeMetric.SPECULATIVE_EXECUTIONS, DriverConfigProfile.DEFAULT_NAME);
      node3Behavior.verifyWrite();
      node3Behavior.setWriteSuccess();

      // No more scheduled tasks since the policy returns 0 on the third call.
      assertThat(harness.nextScheduledTask()).isNull();

      // Note that we don't need to complete any response, the test is just about checking that
      // executions are started.
    }
  }

  @Test
  @UseDataProvider("idempotentConfig")
  public void should_not_start_execution_if_result_complete(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy(DriverConfigProfile.DEFAULT_NAME);
      long firstExecutionDelay = 100L;
      Mockito.when(
              speculativeExecutionPolicy.nextExecution(
                  any(Node.class), eq(null), eq(statement), eq(1)))
          .thenReturn(firstExecutionDelay);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();
      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();

      ScheduledTaskCapturingEventLoop.CapturedTask<?> timeoutFuture = harness.nextScheduledTask();

      // Check that the first execution was scheduled but don't run it yet
      ScheduledTaskCapturingEventLoop.CapturedTask<?> firstExecutionTask =
          harness.nextScheduledTask();
      assertThat(firstExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(firstExecutionDelay);

      // Complete the request from the initial execution
      node1Behavior.setResponseSuccess(defaultFrameOf(singleRow()));
      assertThat(resultSetFuture).isSuccess();

      // Pending speculative executions should have been cancelled. However we don't check
      // firstExecutionTask directly because the request handler's onResponse can sometimes be
      // invoked before operationComplete (this is very unlikely in practice, but happens in our
      // Travis CI build). When that happens, the speculative execution is not recorded yet when
      // cancelScheduledTasks runs.
      // So check the timeout future instead, since it's cancelled in the same method.
      assertThat(timeoutFuture.isCancelled()).isTrue();

      // The fact that we missed the speculative execution is not a problem; even if it starts, it
      // will eventually find out that the result is already complete and cancel itself:
      firstExecutionTask.run();
      node2Behavior.verifyNoWrite();

      Mockito.verify(nodeMetricUpdater1)
          .updateTimer(
              eq(DefaultNodeMetric.CQL_MESSAGES),
              eq(DriverConfigProfile.DEFAULT_NAME),
              anyLong(),
              eq(TimeUnit.NANOSECONDS));
      Mockito.verifyNoMoreInteractions(nodeMetricUpdater1);
    }
  }

  @Test
  @UseDataProvider("idempotentConfig")
  public void should_fail_if_no_nodes(boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    // No configured behaviors => will yield an empty query plan

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy(DriverConfigProfile.DEFAULT_NAME);
      long firstExecutionDelay = 100L;
      Mockito.when(
              speculativeExecutionPolicy.nextExecution(
                  any(Node.class), eq(null), eq(statement), eq(1)))
          .thenReturn(firstExecutionDelay);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();

      harness.nextScheduledTask(); // Discard the timeout task

      assertThat(resultSetFuture)
          .isFailed(error -> assertThat(error).isInstanceOf(NoNodeAvailableException.class));
    }
  }

  @Test
  @UseDataProvider("idempotentConfig")
  public void should_fail_if_no_more_nodes_and_initial_execution_is_last(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    harnessBuilder.withResponse(
        node2,
        defaultFrameOf(new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")));

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy(DriverConfigProfile.DEFAULT_NAME);
      long firstExecutionDelay = 100L;
      Mockito.when(
              speculativeExecutionPolicy.nextExecution(
                  any(Node.class), eq(null), eq(statement), eq(1)))
          .thenReturn(firstExecutionDelay);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();
      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();
      // do not simulate a response from node1 yet

      harness.nextScheduledTask(); // Discard the timeout task

      // Run the next scheduled task to start the speculative execution. node2 will reply with a
      // BOOTSTRAPPING error, causing a RETRY_NEXT; but the query plan is now empty so the
      // speculative execution stops.
      ScheduledTaskCapturingEventLoop.CapturedTask<?> firstExecutionTask =
          harness.nextScheduledTask();
      assertThat(firstExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(firstExecutionDelay);
      firstExecutionTask.run();

      // node1 now replies with the same response, that triggers a RETRY_NEXT
      node1Behavior.setResponseSuccess(
          defaultFrameOf(new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")));

      // But again the query plan is empty so that should fail the request
      assertThat(resultSetFuture)
          .isFailed(
              error -> {
                assertThat(error).isInstanceOf(AllNodesFailedException.class);
                Map<Node, Throwable> nodeErrors = ((AllNodesFailedException) error).getErrors();
                assertThat(nodeErrors).containsOnlyKeys(node1, node2);
                assertThat(nodeErrors.get(node1)).isInstanceOf(BootstrappingException.class);
                assertThat(nodeErrors.get(node2)).isInstanceOf(BootstrappingException.class);
              });
    }
  }

  @Test
  @UseDataProvider("idempotentConfig")
  public void should_fail_if_no_more_nodes_and_speculative_execution_is_last(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy(DriverConfigProfile.DEFAULT_NAME);
      long firstExecutionDelay = 100L;
      Mockito.when(
              speculativeExecutionPolicy.nextExecution(
                  any(Node.class), eq(null), eq(statement), eq(1)))
          .thenReturn(firstExecutionDelay);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();
      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();
      // do not simulate a response from node1 yet

      harness.nextScheduledTask(); // Discard the timeout task

      // Run the next scheduled task to start the speculative execution.
      ScheduledTaskCapturingEventLoop.CapturedTask<?> firstExecutionTask =
          harness.nextScheduledTask();
      assertThat(firstExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(firstExecutionDelay);
      firstExecutionTask.run();

      // node1 now replies with a BOOTSTRAPPING error that triggers a RETRY_NEXT
      // but the query plan is empty so the initial execution stops
      node1Behavior.setResponseSuccess(
          defaultFrameOf(new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")));

      // Same thing with node2, so the speculative execution should reach the end of the query plan
      // and fail the request
      node2Behavior.setResponseSuccess(
          defaultFrameOf(new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")));

      assertThat(resultSetFuture)
          .isFailed(
              error -> {
                assertThat(error).isInstanceOf(AllNodesFailedException.class);
                Map<Node, Throwable> nodeErrors = ((AllNodesFailedException) error).getErrors();
                assertThat(nodeErrors).containsOnlyKeys(node1, node2);
                assertThat(nodeErrors.get(node1)).isInstanceOf(BootstrappingException.class);
                assertThat(nodeErrors.get(node2)).isInstanceOf(BootstrappingException.class);
              });
    }
  }

  @Test
  @UseDataProvider("idempotentConfig")
  public void should_retry_in_speculative_executions(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    harnessBuilder.withResponse(node3, defaultFrameOf(singleRow()));

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy(DriverConfigProfile.DEFAULT_NAME);
      long firstExecutionDelay = 100L;
      Mockito.when(
              speculativeExecutionPolicy.nextExecution(
                  any(Node.class), eq(null), eq(statement), eq(1)))
          .thenReturn(firstExecutionDelay);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();
      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();
      // do not simulate a response from node1. The request will stay hanging for the rest of this
      // test

      harness.nextScheduledTask(); // Discard the timeout task

      ScheduledTaskCapturingEventLoop.CapturedTask<?> firstExecutionTask =
          harness.nextScheduledTask();
      assertThat(firstExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(firstExecutionDelay);
      firstExecutionTask.run();
      node2Behavior.verifyWrite();
      node2Behavior.setWriteSuccess();

      // node2 replies with a response that triggers a RETRY_NEXT
      node2Behavior.setResponseSuccess(
          defaultFrameOf(new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")));

      // The second execution should move to node3 and complete the request
      assertThat(resultSetFuture).isSuccess();

      // The request to node1 was still in flight, it should have been cancelled
      node1Behavior.verifyCancellation();
    }
  }

  @Test
  @UseDataProvider("idempotentConfig")
  public void should_stop_retrying_other_executions_if_result_complete(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy(DriverConfigProfile.DEFAULT_NAME);
      long firstExecutionDelay = 100L;
      Mockito.when(
              speculativeExecutionPolicy.nextExecution(
                  any(Node.class), eq(null), eq(statement), eq(1)))
          .thenReturn(firstExecutionDelay);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(statement, harness.getSession(), harness.getContext(), "test")
              .handle();
      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();

      harness.nextScheduledTask(); // Discard the timeout task

      ScheduledTaskCapturingEventLoop.CapturedTask<?> firstExecutionTask =
          harness.nextScheduledTask();
      assertThat(firstExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(firstExecutionDelay);
      firstExecutionTask.run();
      node2Behavior.verifyWrite();
      node2Behavior.setWriteSuccess();

      // Complete the request from the initial execution
      node1Behavior.setResponseSuccess(defaultFrameOf(singleRow()));
      assertThat(resultSetFuture).isSuccess();

      // node2 replies with a response that would trigger a RETRY_NEXT if the request was still
      // running
      node2Behavior.setResponseSuccess(
          defaultFrameOf(new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")));

      // The speculative execution should not move to node3 because it is stopped
      node3Behavior.verifyNoWrite();
    }
  }
}
