/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.internal.core.cql.continuous;

import static com.datastax.dse.driver.api.core.DseProtocolVersion.DSE_V1;
import static com.datastax.dse.driver.api.core.DseProtocolVersion.DSE_V2;
import static com.datastax.dse.protocol.internal.DseProtocolConstants.RevisionType.CANCEL_CONTINUOUS_PAGING;
import static com.datastax.dse.protocol.internal.DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES;
import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.DseTestDataProviders;
import com.datastax.dse.driver.DseTestFixtures;
import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.protocol.internal.request.Revise;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.ProtocolFeature;
import com.datastax.oss.driver.internal.core.cql.PoolBehavior;
import com.datastax.oss.driver.internal.core.cql.RequestHandlerTestHarness;
import com.datastax.oss.driver.internal.core.util.concurrent.CapturingTimer.CapturedTimeout;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.junit.Test;
import org.mockito.Mockito;

public class ContinuousCqlRequestHandlerTest extends ContinuousCqlRequestHandlerTestBase {

  private static final Pattern LOG_PREFIX_PER_REQUEST = Pattern.compile("test\\|\\d*\\|\\d");

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_complete_single_page_result(DseProtocolVersion version) {
    try (RequestHandlerTestHarness harness =
        continuousHarnessBuilder()
            .withProtocolVersion(version)
            .withResponse(node1, defaultFrameOf(DseTestFixtures.singleDseRow()))
            .build()) {

      CompletionStage<ContinuousAsyncResultSet> resultSetFuture =
          new ContinuousCqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThatStage(resultSetFuture)
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
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_complete_multi_page_result(DseProtocolVersion version) {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(version);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (RequestHandlerTestHarness harness = builder.build()) {

      ContinuousCqlRequestHandler handler =
          new ContinuousCqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");
      CompletionStage<ContinuousAsyncResultSet> page1Future = handler.handle();

      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(1, false)));

      assertThatStage(page1Future)
          .isSuccess(
              page1 -> {
                assertThat(page1.hasMorePages()).isTrue();
                assertThat(page1.pageNumber()).isEqualTo(1);
                Iterator<Row> rows = page1.currentPage().iterator();
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows).toIterable().hasSize(10);
                ExecutionInfo executionInfo = page1.getExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node1);
                assertThat(executionInfo.getErrors()).isEmpty();
                assertThat(executionInfo.getIncomingPayload()).isEmpty();
                assertThat(executionInfo.getPagingState()).isNotNull();
                assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
                assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
                assertThat(executionInfo.getWarnings()).isEmpty();
              });

      ContinuousAsyncResultSet page1 = CompletableFutures.getCompleted(page1Future);
      assertThat(handler.getPendingResult()).isNull();
      CompletionStage<ContinuousAsyncResultSet> page2Future = page1.fetchNextPage();
      assertThat(handler.getPendingResult()).isNotNull();
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(2, true)));

      assertThatStage(page2Future)
          .isSuccess(
              page2 -> {
                assertThat(page2.hasMorePages()).isFalse();
                assertThat(page2.pageNumber()).isEqualTo(2);
                Iterator<Row> rows = page2.currentPage().iterator();
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows).toIterable().hasSize(10);
                ExecutionInfo executionInfo = page2.getExecutionInfo();
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
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_fail_if_no_node_available(DseProtocolVersion version) {
    try (RequestHandlerTestHarness harness =
        continuousHarnessBuilder()
            .withProtocolVersion(version)
            // Mock no responses => this will produce an empty query plan
            .build()) {

      CompletionStage<ContinuousAsyncResultSet> resultSetFuture =
          new ContinuousCqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isFailed(error -> assertThat(error).isInstanceOf(NoNodeAvailableException.class));
    }
  }

  @Test
  @UseDataProvider(value = "allOssProtocolVersions", location = DseTestDataProviders.class)
  public void should_throw_if_protocol_version_does_not_support_continuous_paging(
      ProtocolVersion version) {
    try (RequestHandlerTestHarness harness =
        continuousHarnessBuilder().withProtocolVersion(version).build()) {
      Mockito.when(
              harness
                  .getContext()
                  .getProtocolVersionRegistry()
                  .supports(any(DefaultProtocolVersion.class), any(ProtocolFeature.class)))
          .thenReturn(false);
      assertThatThrownBy(
              () ->
                  new ContinuousCqlRequestHandler(
                          UNDEFINED_IDEMPOTENCE_STATEMENT,
                          harness.getSession(),
                          harness.getContext(),
                          "test")
                      .handle())
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Cannot execute continuous paging requests with protocol version " + version);
    }
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_time_out_if_first_page_takes_too_long(DseProtocolVersion version)
      throws Exception {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(version);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (RequestHandlerTestHarness harness = builder.build()) {

      CompletionStage<ContinuousAsyncResultSet> resultSetFuture =
          new ContinuousCqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      // mark the initial request as successful, which should schedule a timeout for the first page
      node1Behavior.setWriteSuccess();
      CapturedTimeout page1Timeout = harness.nextScheduledTimeout();
      assertThat(page1Timeout.getDelay(TimeUnit.NANOSECONDS))
          .isEqualTo(TIMEOUT_FIRST_PAGE.toNanos());

      page1Timeout.task().run(page1Timeout);

      assertThatStage(resultSetFuture)
          .isFailed(
              t ->
                  assertThat(t)
                      .isInstanceOf(DriverTimeoutException.class)
                      .hasMessageContaining("Timed out waiting for page 1"));
    }
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_time_out_if_other_page_takes_too_long(DseProtocolVersion version)
      throws Exception {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(version);
    PoolBehavior node1Behavior = builder.customBehavior(node1);

    try (RequestHandlerTestHarness harness = builder.build()) {

      CompletionStage<ContinuousAsyncResultSet> page1Future =
          new ContinuousCqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      // mark the initial request as successful, which should schedule a timeout for the first page
      node1Behavior.setWriteSuccess();
      CapturedTimeout page1Timeout = harness.nextScheduledTimeout();
      assertThat(page1Timeout.getDelay(TimeUnit.NANOSECONDS))
          .isEqualTo(TIMEOUT_FIRST_PAGE.toNanos());

      // the server replies with page 1, the corresponding timeout should be cancelled
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(1, false)));
      assertThat(page1Timeout.isCancelled()).isTrue();

      // request page 2, the queue is empty so this should request more pages and schedule another
      // timeout
      ContinuousAsyncResultSet page1 = CompletableFutures.getUninterruptibly(page1Future);
      CompletionStage<ContinuousAsyncResultSet> page2Future = page1.fetchNextPage();
      CapturedTimeout page2Timeout = harness.nextScheduledTimeout();
      assertThat(page2Timeout.getDelay(TimeUnit.NANOSECONDS))
          .isEqualTo(TIMEOUT_OTHER_PAGES.toNanos());

      page2Timeout.task().run(page2Timeout);

      assertThatStage(page2Future)
          .isFailed(
              t ->
                  assertThat(t)
                      .isInstanceOf(DriverTimeoutException.class)
                      .hasMessageContaining("Timed out waiting for page 2"));
    }
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_cancel_future_if_session_cancelled(DseProtocolVersion version) {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(version);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (RequestHandlerTestHarness harness = builder.build()) {

      ContinuousCqlRequestHandler handler =
          new ContinuousCqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");
      CompletionStage<ContinuousAsyncResultSet> page1Future = handler.handle();

      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(1, false)));
      // will be discarded
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(2, false)));

      ContinuousAsyncResultSet page1 = CompletableFutures.getUninterruptibly(page1Future);
      page1.cancel();

      assertThat(handler.getState()).isEqualTo(-2);
      assertThat(page1.fetchNextPage()).isCancelled();
    }
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_cancel_session_if_future_cancelled(DseProtocolVersion version) {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(version);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (RequestHandlerTestHarness harness = builder.build()) {

      ContinuousCqlRequestHandler handler =
          new ContinuousCqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");
      CompletionStage<ContinuousAsyncResultSet> page1Future = handler.handle();

      page1Future.toCompletableFuture().cancel(true);
      // this should be ignored
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(1, false)));
      assertThat(handler.getState()).isEqualTo(-2);
    }
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_not_cancel_session_if_future_cancelled_but_already_done(
      DseProtocolVersion version) {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(version);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (RequestHandlerTestHarness harness = builder.build()) {

      ContinuousCqlRequestHandler handler =
          new ContinuousCqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");
      CompletionStage<ContinuousAsyncResultSet> page1Future = handler.handle();

      // this will complete page 1 future
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(1, true)));

      // to late
      page1Future.toCompletableFuture().cancel(true);
      assertThat(handler.getState()).isEqualTo(-1);
    }
  }

  @Test
  public void should_send_cancel_request_if_dse_v2() {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(DSE_V2);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (RequestHandlerTestHarness harness = builder.build()) {

      ContinuousCqlRequestHandler handler =
          new ContinuousCqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");
      CompletionStage<ContinuousAsyncResultSet> page1Future = handler.handle();

      page1Future.toCompletableFuture().cancel(true);
      assertThat(handler.getState()).isEqualTo(-2);
      verify(node1Behavior.getChannel())
          .write(argThat(this::isCancelRequest), anyBoolean(), anyMap(), any());
    }
  }

  @Test
  public void should_toggle_channel_autoread_if_dse_v1() {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(DSE_V1);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (RequestHandlerTestHarness harness = builder.build()) {

      CompletionStage<ContinuousAsyncResultSet> page1Future =
          new ContinuousCqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      // simulate the arrival of 5 pages, the first one will complete page1 future above,
      // the following 4 will be enqueued and should trigger autoread off
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(1, false)));
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(2, false)));
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(3, false)));
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(4, false)));
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(5, false)));

      verify(node1Behavior.getChannel().config()).setAutoRead(false);

      // simulate the retrieval of 2 pages, this should dequeue page 2
      // and trigger autoread on
      ContinuousAsyncResultSet page1 = CompletableFutures.getCompleted(page1Future);
      CompletableFutures.getCompleted(page1.fetchNextPage());

      verify(node1Behavior.getChannel().config()).setAutoRead(true);

      // in DSE_V1, the backpressure request should not have been sent
      verify(node1Behavior.getChannel(), never())
          .write(any(Revise.class), anyBoolean(), anyMap(), any());
    }
  }

  @Test
  public void should_send_backpressure_request_if_dse_v2() {
    RequestHandlerTestHarness.Builder builder =
        continuousHarnessBuilder().withProtocolVersion(DSE_V2);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (RequestHandlerTestHarness harness = builder.build()) {

      CompletionStage<ContinuousAsyncResultSet> page1Future =
          new ContinuousCqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      // simulate the arrival of 4 pages, the first one will complete page1 future above,
      // the following 3 will be enqueued
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(1, false)));
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(2, false)));
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(3, false)));
      node1Behavior.setResponseSuccess(defaultFrameOf(DseTestFixtures.tenDseRows(4, false)));

      // simulate the retrieval of 2 pages, this should dequeue page 2
      // and trigger a backpressure request as the queue is now half empty (2/4)
      ContinuousAsyncResultSet page1 = CompletableFutures.getCompleted(page1Future);
      CompletableFutures.getCompleted(page1.fetchNextPage());

      verify(node1Behavior.getChannel())
          .write(argThat(this::isBackpressureRequest), anyBoolean(), anyMap(), any());
      // should not mess with autoread in dse v2
      verify(node1Behavior.getChannel().config(), never()).setAutoRead(anyBoolean());
    }
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_invoke_request_tracker(DseProtocolVersion version) {
    try (RequestHandlerTestHarness harness =
        continuousHarnessBuilder()
            .withProtocolVersion(version)
            .withResponse(
                node1,
                defaultFrameOf(
                    new com.datastax.oss.protocol.internal.response.Error(
                        ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")))
            .withResponse(node2, defaultFrameOf(DseTestFixtures.singleDseRow()))
            .build()) {

      RequestTracker requestTracker = mock(RequestTracker.class);
      when(harness.getContext().getRequestTracker()).thenReturn(requestTracker);

      CompletionStage<ContinuousAsyncResultSet> resultSetFuture =
          new ContinuousCqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(
              resultSet -> {
                Iterator<Row> rows = resultSet.currentPage().iterator();
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getString("message")).isEqualTo("hello, world");
                ExecutionInfo executionInfo = resultSet.getExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node2);
                assertThat(executionInfo.getErrors()).isNotEmpty();
                assertThat(executionInfo.getIncomingPayload()).isEmpty();
                assertThat(executionInfo.getPagingState()).isNull();
                assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
                assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
                assertThat(executionInfo.getWarnings()).isEmpty();

                verify(requestTracker)
                    .onNodeError(
                        eq(UNDEFINED_IDEMPOTENCE_STATEMENT),
                        any(BootstrappingException.class),
                        anyLong(),
                        any(DriverExecutionProfile.class),
                        eq(node1),
                        nullable(ExecutionInfo.class),
                        matches(LOG_PREFIX_PER_REQUEST));
                verify(requestTracker)
                    .onNodeSuccess(
                        eq(UNDEFINED_IDEMPOTENCE_STATEMENT),
                        anyLong(),
                        any(DriverExecutionProfile.class),
                        eq(node2),
                        any(ExecutionInfo.class),
                        matches(LOG_PREFIX_PER_REQUEST));
                verify(requestTracker)
                    .onSuccess(
                        eq(UNDEFINED_IDEMPOTENCE_STATEMENT),
                        anyLong(),
                        any(DriverExecutionProfile.class),
                        eq(node2),
                        any(ExecutionInfo.class),
                        matches(LOG_PREFIX_PER_REQUEST));
                verifyNoMoreInteractions(requestTracker);
              });
    }
  }

  private boolean isBackpressureRequest(Message argument) {
    return argument instanceof Revise && ((Revise) argument).revisionType == MORE_CONTINUOUS_PAGES;
  }

  private boolean isCancelRequest(Message argument) {
    return argument instanceof Revise
        && ((Revise) argument).revisionType == CANCEL_CONTINUOUS_PAGING;
  }
}
