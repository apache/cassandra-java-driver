/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.util.concurrent.ScheduledTaskCapturingEventLoop;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class CqlRequestHandlerSpeculativeExecutionTest extends CqlRequestHandlerTestBase {

  @Test(dataProvider = "nonIdempotentConfig")
  public void should_not_schedule_speculative_executions_if_not_idempotent(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy();

      new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
          .asyncResult();

      node1Behavior.verifyWrite();

      harness.nextScheduledTask(); // Discard the timeout task
      assertThat(harness.nextScheduledTask()).isNull();

      Mockito.verifyNoMoreInteractions(speculativeExecutionPolicy);
    }
  }

  @Test(dataProvider = "idempotentConfig")
  public void should_schedule_speculative_executions(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy();
      long firstExecutionDelay = 100L;
      long secondExecutionDelay = 200L;
      Mockito.when(speculativeExecutionPolicy.nextExecution(null, statement, 1))
          .thenReturn(firstExecutionDelay);
      Mockito.when(speculativeExecutionPolicy.nextExecution(null, statement, 2))
          .thenReturn(secondExecutionDelay);
      Mockito.when(speculativeExecutionPolicy.nextExecution(null, statement, 3)).thenReturn(0L);

      new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
          .asyncResult();

      node1Behavior.verifyWrite();

      harness.nextScheduledTask(); // Discard the timeout task

      ScheduledTaskCapturingEventLoop.CapturedTask<?> firstExecutionTask =
          harness.nextScheduledTask();
      assertThat(firstExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(firstExecutionDelay);
      firstExecutionTask.run();
      node2Behavior.verifyWrite();

      ScheduledTaskCapturingEventLoop.CapturedTask<?> secondExecutionTask =
          harness.nextScheduledTask();
      assertThat(secondExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(secondExecutionDelay);
      secondExecutionTask.run();
      node3Behavior.verifyWrite();

      // No more scheduled tasks since the policy returns 0 on the third call.
      assertThat(harness.nextScheduledTask()).isNull();

      // Note that we don't need to complete any response, the test is just about checking that
      // executions are started.
    }
  }

  @Test(dataProvider = "idempotentConfig")
  public void should_not_start_execution_if_result_complete(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy();
      long firstExecutionDelay = 100L;
      Mockito.when(speculativeExecutionPolicy.nextExecution(null, statement, 1))
          .thenReturn(firstExecutionDelay);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
              .asyncResult();
      node1Behavior.verifyWrite();

      harness.nextScheduledTask(); // Discard the timeout task

      // Check that the first execution was scheduled but don't run it yet
      ScheduledTaskCapturingEventLoop.CapturedTask<?> firstExecutionTask =
          harness.nextScheduledTask();
      assertThat(firstExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(firstExecutionDelay);

      // Complete the request from the initial execution
      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(defaultFrameOf(singleRow()));
      assertThat(resultSetFuture).isSuccess();

      // The speculative execution should have been cancelled
      assertThat(firstExecutionTask.isCancelled()).isTrue();

      // Run the task anyway; we're bending reality a bit here since the task is already cancelled
      // and wouldn't run, but this allows us to test the case where the completion races with the
      // start of the task.
      firstExecutionTask.run();
      node2Behavior.verifyNoWrite();
    }
  }

  @Test(dataProvider = "idempotentConfig")
  public void should_retry_in_speculative_executions(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    harnessBuilder.withResponse(node3, defaultFrameOf(singleRow()));

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy();
      long firstExecutionDelay = 100L;
      Mockito.when(speculativeExecutionPolicy.nextExecution(null, statement, 1))
          .thenReturn(firstExecutionDelay);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
              .asyncResult();
      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();
      // do not simulate a response from node1. The request will stay hanging for the rest of this test

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

  @Test(dataProvider = "idempotentConfig")
  public void should_stop_retrying_other_executions_if_result_complete(
      boolean defaultIdempotence, SimpleStatement statement) {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withDefaultIdempotence(defaultIdempotence);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      SpeculativeExecutionPolicy speculativeExecutionPolicy =
          harness.getContext().speculativeExecutionPolicy();
      long firstExecutionDelay = 100L;
      Mockito.when(speculativeExecutionPolicy.nextExecution(null, statement, 1))
          .thenReturn(firstExecutionDelay);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(statement, harness.getSession(), harness.getContext(), "test")
              .asyncResult();
      node1Behavior.verifyWrite();

      harness.nextScheduledTask(); // Discard the timeout task

      ScheduledTaskCapturingEventLoop.CapturedTask<?> firstExecutionTask =
          harness.nextScheduledTask();
      assertThat(firstExecutionTask.getInitialDelay(TimeUnit.MILLISECONDS))
          .isEqualTo(firstExecutionDelay);
      firstExecutionTask.run();
      node2Behavior.verifyWrite();
      node2Behavior.setWriteSuccess();

      // Complete the request from the initial execution
      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(defaultFrameOf(singleRow()));
      assertThat(resultSetFuture).isSuccess();

      // node2 replies with a response that would trigger a RETRY_NEXT if the request was still running
      node2Behavior.setResponseSuccess(
          defaultFrameOf(new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")));

      // The speculative execution should not move to node3 because it is stopped
      node3Behavior.verifyNoWrite();
    }
  }
}
