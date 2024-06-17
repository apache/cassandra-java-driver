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

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static com.datastax.oss.protocol.internal.Frame.NO_PAYLOAD;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.DseTestDataProviders;
import com.datastax.dse.driver.DseTestFixtures;
import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.UnexpectedResponseException;
import com.datastax.oss.driver.internal.core.cql.RequestHandlerTestHarness;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import io.netty.util.concurrent.Future;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.Test;
import org.mockito.Mock;

public class ContinuousCqlRequestHandlerReprepareTest extends ContinuousCqlRequestHandlerTestBase {

  private final byte[] preparedId = {1, 2, 3};
  private final ByteBuffer preparedIdBuf = ByteBuffer.wrap(preparedId);

  private final RepreparePayload repreparePayload =
      new RepreparePayload(preparedIdBuf, "irrelevant", CqlIdentifier.fromCql("ks"), NO_PAYLOAD);

  private final ConcurrentMap<ByteBuffer, RepreparePayload> repreparePayloads =
      new ConcurrentHashMap<>(ImmutableMap.of(preparedIdBuf, repreparePayload));

  private final Unprepared unprepared = new Unprepared("test", preparedId);
  private final Prepared prepared = new Prepared(preparedId, null, null, null);
  private final Error unrecoverable =
      new Error(ProtocolConstants.ErrorCode.SYNTAX_ERROR, "bad query");
  private final Error recoverable = new Error(ErrorCode.SERVER_ERROR, "sorry");

  @Mock private Future<Void> future;

  @Override
  public void setup() {
    super.setup();
    when(future.isSuccess()).thenReturn(true);
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_prepare_and_retry_on_same_node(DseProtocolVersion version) {

    try (RequestHandlerTestHarness harness =
        continuousHarnessBuilder()
            .withResponse(node1, defaultFrameOf(unprepared))
            .withProtocolVersion(version)
            .build()) {

      when(harness.getSession().getRepreparePayloads()).thenReturn(repreparePayloads);
      when(harness.getChannel(node1).write(any(Prepare.class), anyBoolean(), anyMap(), any()))
          .then(
              invocation -> {
                AdminRequestHandler<?> admin = invocation.getArgument(3);
                admin.onResponse(defaultFrameOf(prepared));
                return future;
              });

      new ContinuousCqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test")
          .handle();

      verify(harness.getChannel(node1)).write(any(Prepare.class), anyBoolean(), anyMap(), any());
      // should have attempted to execute the query twice on the same node
      verify(harness.getChannel(node1), times(2))
          .write(any(Query.class), anyBoolean(), anyMap(), any());
    }
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_abort_when_prepare_fails_with_unrecoverable_error(DseProtocolVersion version) {

    try (RequestHandlerTestHarness harness =
        continuousHarnessBuilder()
            .withResponse(node1, defaultFrameOf(unprepared))
            .withProtocolVersion(version)
            .build()) {

      when(harness.getSession().getRepreparePayloads()).thenReturn(repreparePayloads);
      when(harness.getChannel(node1).write(any(Prepare.class), anyBoolean(), anyMap(), any()))
          .then(
              invocation -> {
                AdminRequestHandler<?> admin = invocation.getArgument(3);
                admin.onResponse(defaultFrameOf(unrecoverable));
                return future;
              });

      ContinuousCqlRequestHandler handler =
          new ContinuousCqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");
      CompletionStage<ContinuousAsyncResultSet> page1Future = handler.handle();

      verify(harness.getChannel(node1)).write(any(Query.class), anyBoolean(), anyMap(), any());
      verify(harness.getChannel(node1)).write(any(Prepare.class), anyBoolean(), anyMap(), any());

      assertThat(handler.getState()).isEqualTo(-2);
      assertThat(page1Future).isCompletedExceptionally();
      Throwable t = catchThrowable(() -> page1Future.toCompletableFuture().get());
      assertThat(t).hasRootCauseInstanceOf(SyntaxError.class).hasMessageContaining("bad query");
    }
  }

  @Test
  @UseDataProvider(value = "allDseProtocolVersions", location = DseTestDataProviders.class)
  public void should_try_next_node_when_prepare_fails_with_recoverable_error(
      DseProtocolVersion version) {

    try (RequestHandlerTestHarness harness =
        continuousHarnessBuilder()
            .withResponse(node1, defaultFrameOf(unprepared))
            .withResponse(node2, defaultFrameOf(DseTestFixtures.singleDseRow()))
            .withProtocolVersion(version)
            .build()) {

      when(harness.getSession().getRepreparePayloads()).thenReturn(repreparePayloads);
      when(harness.getChannel(node1).write(any(Prepare.class), anyBoolean(), anyMap(), any()))
          .then(
              invocation -> {
                AdminRequestHandler<?> admin = invocation.getArgument(3);
                admin.onResponse(defaultFrameOf(recoverable));
                return future;
              });

      ContinuousCqlRequestHandler handler =
          new ContinuousCqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");
      CompletionStage<ContinuousAsyncResultSet> page1Future = handler.handle();

      verify(harness.getChannel(node1)).write(any(Query.class), anyBoolean(), anyMap(), any());
      verify(harness.getChannel(node1)).write(any(Prepare.class), anyBoolean(), anyMap(), any());
      // should have tried the next host
      verify(harness.getChannel(node2)).write(any(Query.class), anyBoolean(), anyMap(), any());

      assertThat(handler.getState()).isEqualTo(-1);
      assertThatStage(page1Future)
          .isSuccess(
              rs -> {
                assertThat(rs.currentPage()).hasSize(1);
                assertThat(rs.hasMorePages()).isFalse();
                assertThat(rs.getExecutionInfo().getCoordinator()).isEqualTo(node2);
                assertThat(rs.getExecutionInfo().getErrors())
                    .hasSize(1)
                    .allSatisfy(
                        entry -> {
                          assertThat(entry.getKey()).isEqualTo(node1);
                          assertThat(entry.getValue())
                              .isInstanceOf(UnexpectedResponseException.class)
                              .hasMessageContaining(recoverable.toString());
                        });
              });
    }
  }
}
