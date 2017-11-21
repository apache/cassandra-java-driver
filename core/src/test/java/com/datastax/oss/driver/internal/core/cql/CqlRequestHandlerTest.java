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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import com.datastax.oss.driver.internal.core.util.concurrent.ScheduledTaskCapturingEventLoop;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

public class CqlRequestHandlerTest extends CqlRequestHandlerTestBase {

  @Test
  public void should_complete_result_if_first_node_replies_immediately() {
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            .withResponse(node1, defaultFrameOf(singleRow()))
            .build()) {

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThat(resultSetFuture)
          .isSuccess(
              resultSet -> {
                Iterator<Row> rows = resultSet.currentPage().iterator();
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getString("message")).isEqualTo("hello, world");

                ExecutionInfo executionInfo = resultSet.getExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node1);
                assertThat(executionInfo.getErrors()).isEmpty();
                assertThat(executionInfo.getIncomingPayload()).isEmpty();
                assertThat(executionInfo.getPagingState()).isNull();
                assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
                assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
                assertThat(executionInfo.getWarnings()).isEmpty();
              });
    }
  }

  @Test
  public void should_fail_if_no_node_available() {
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            // Mock no responses => this will produce an empty query plan
            .build()) {

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThat(resultSetFuture)
          .isFailed(error -> assertThat(error).isInstanceOf(NoNodeAvailableException.class));
    }
  }

  @Test
  public void should_time_out_if_first_node_takes_too_long_to_respond() {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    node1Behavior.setWriteSuccess();

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      // First scheduled task is the timeout, run it before node1 has responded
      ScheduledTaskCapturingEventLoop.CapturedTask<?> scheduledTask = harness.nextScheduledTask();
      Duration configuredTimeout =
          harness
              .getContext()
              .config()
              .getDefaultProfile()
              .getDuration(CoreDriverOption.REQUEST_TIMEOUT);
      assertThat(scheduledTask.getInitialDelay(TimeUnit.NANOSECONDS))
          .isEqualTo(configuredTimeout.toNanos());
      scheduledTask.run();

      assertThat(resultSetFuture)
          .isFailed(t -> assertThat(t).isInstanceOf(DriverTimeoutException.class));
    }
  }

  @Test
  public void should_switch_keyspace_on_session_after_successful_use_statement() {
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            .withResponse(node1, defaultFrameOf(new SetKeyspace("newKeyspace")))
            .build()) {

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThat(resultSetFuture)
          .isSuccess(
              resultSet ->
                  Mockito.verify(harness.getSession())
                      .setKeyspace(CqlIdentifier.fromInternal("newKeyspace")));
    }
  }

  @Test
  public void should_reprepare_on_the_fly_if_not_prepared() throws InterruptedException {
    ByteBuffer mockId = Bytes.fromHexString("0xffff");

    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    Mockito.when(preparedStatement.getId()).thenReturn(mockId);
    BoundStatement boundStatement = Mockito.mock(BoundStatement.class);
    Mockito.when(boundStatement.getPreparedStatement()).thenReturn(preparedStatement);
    Mockito.when(boundStatement.getValues()).thenReturn(Collections.emptyList());

    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    // For the first attempt that gets the UNPREPARED response
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    // For the second attempt that succeeds
    harnessBuilder.withResponse(node1, defaultFrameOf(singleRow()));

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      // The handler will look for the info to reprepare in the session's cache, put it there
      ConcurrentMap<ByteBuffer, RepreparePayload> repreparePayloads = new ConcurrentHashMap<>();
      repreparePayloads.put(
          mockId, new RepreparePayload(mockId, "mock query", null, Collections.emptyMap()));
      Mockito.when(harness.getSession().getRepreparePayloads()).thenReturn(repreparePayloads);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestAsyncHandler(
                  boundStatement, harness.getSession(), harness.getContext(), "test")
              .handle();

      // Before we proceed, mock the PREPARE exchange that will occur as soon as we complete the
      // first response.
      node1Behavior.mockFollowupRequest(
          Prepare.class, defaultFrameOf(new Prepared(mockId.array(), null, null, null)));

      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(
          defaultFrameOf(new Unprepared("mock message", mockId.array())));

      // Should now re-prepare, re-execute and succeed.
      assertThat(resultSetFuture).isSuccess();
    }
  }
}
