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
import static org.mockito.ArgumentMatchers.eq;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class CqlPrepareHandlerTest {

  private static final DefaultPrepareRequest PREPARE_REQUEST =
      new DefaultPrepareRequest("mock query");

  @Mock private Node node1;
  @Mock private Node node2;
  @Mock private Node node3;

  private ConcurrentMap<ByteBuffer, DefaultPreparedStatement> preparedStatementsCache =
      new ConcurrentHashMap<>();

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    // By default, simulate that the prepared statement is not already in the driver's cache
    preparedStatementsCache.clear();
  }

  @Test
  public void should_prepare_on_first_node_and_reprepare_on_others() {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareAsyncHandler(
                  PREPARE_REQUEST,
                  preparedStatementsCache,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      // The future waits for the reprepare attempt on other nodes, so it's not done yet.
      assertThat(prepareFuture).isNotDone();

      // Should now reprepare on the remaining nodes:
      node2Behavior.verifyWrite();
      node2Behavior.setWriteSuccess();
      node2Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      node3Behavior.verifyWrite();
      node3Behavior.setWriteSuccess();
      node3Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      assertThat(prepareFuture).isSuccess(CqlPrepareHandlerTest::assertMatchesSimplePrepared);
    }
  }

  @Test
  public void should_not_reprepare_on_other_nodes_if_disabled_in_config() {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      DriverConfigProfile config = harness.getContext().config().getDefaultProfile();
      Mockito.when(config.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES)).thenReturn(false);

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareAsyncHandler(
                  PREPARE_REQUEST,
                  preparedStatementsCache,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      // The future should complete immediately:
      assertThat(prepareFuture).isSuccess();

      // And the other nodes should not be contacted:
      node2Behavior.verifyNoWrite();
      node3Behavior.verifyNoWrite();
    }
  }

  @Test
  public void should_not_reprepare_on_other_nodes_if_already_cached() {
    // Simulate an existing entry in the driver's cache:
    DefaultPreparedStatement mockExistingStatement = Mockito.mock(DefaultPreparedStatement.class);
    preparedStatementsCache.put(Bytes.fromHexString("0xffff"), mockExistingStatement);

    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareAsyncHandler(
                  PREPARE_REQUEST,
                  preparedStatementsCache,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      // When the statement already existed, we don't prepare on other nodes, so the future should
      // complete immediately.
      assertThat(prepareFuture)
          .isSuccess(
              preparedStatement -> assertThat(preparedStatement).isSameAs(mockExistingStatement));

      // And the other nodes should not be contacted:
      node2Behavior.verifyNoWrite();
      node3Behavior.verifyNoWrite();
    }
  }

  @Test
  public void should_ignore_errors_while_repreparing_on_other_nodes() {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withResponse(node1, defaultFrameOf(simplePrepared()));
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareAsyncHandler(
                  PREPARE_REQUEST,
                  preparedStatementsCache,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThat(prepareFuture).isNotDone();

      // Other nodes fail, the future should still succeed when all done
      node2Behavior.verifyWrite();
      node2Behavior.setWriteSuccess();
      node2Behavior.setResponseSuccess(
          defaultFrameOf(new Error(ProtocolConstants.ErrorCode.SERVER_ERROR, "mock error")));

      node3Behavior.verifyWrite();
      node3Behavior.setWriteFailure(new RuntimeException("mock error"));

      assertThat(prepareFuture).isSuccess(CqlPrepareHandlerTest::assertMatchesSimplePrepared);
    }
  }

  @Test
  public void should_retry_initial_prepare_if_recoverable_error() {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder()
            .withResponse(
                node1,
                defaultFrameOf(new Error(ProtocolConstants.ErrorCode.OVERLOADED, "mock message")))
            .withResponse(node2, defaultFrameOf(simplePrepared()));
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      // Make node1's error recoverable, will switch to node2
      Mockito.when(
              harness
                  .getContext()
                  .retryPolicy()
                  .onErrorResponse(eq(PREPARE_REQUEST), any(OverloadedException.class), eq(0)))
          .thenReturn(RetryDecision.RETRY_NEXT);

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareAsyncHandler(
                  PREPARE_REQUEST,
                  preparedStatementsCache,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      // Success on node2, reprepare on node3
      assertThat(prepareFuture).isNotDone();
      node3Behavior.verifyWrite();
      node3Behavior.setWriteSuccess();
      node3Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      assertThat(prepareFuture).isSuccess(CqlPrepareHandlerTest::assertMatchesSimplePrepared);
    }
  }

  @Test
  public void should_not_retry_initial_prepare_if_unrecoverable_error() {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder()
            .withResponse(
                node1,
                defaultFrameOf(new Error(ProtocolConstants.ErrorCode.OVERLOADED, "mock message")));
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      // Make node1's error unrecoverable, will rethrow
      Mockito.when(
              harness
                  .getContext()
                  .retryPolicy()
                  .onErrorResponse(eq(PREPARE_REQUEST), any(OverloadedException.class), eq(0)))
          .thenReturn(RetryDecision.RETHROW);

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareAsyncHandler(
                  PREPARE_REQUEST,
                  preparedStatementsCache,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      // Success on node2, reprepare on node3
      assertThat(prepareFuture)
          .isFailed(
              error -> {
                assertThat(error).isInstanceOf(OverloadedException.class);
                node2Behavior.verifyNoWrite();
                node3Behavior.verifyNoWrite();
              });
    }
  }

  @Test
  public void should_fail_if_retry_policy_ignores_error() {
    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder()
            .withResponse(
                node1,
                defaultFrameOf(new Error(ProtocolConstants.ErrorCode.OVERLOADED, "mock message")));
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      // Make node1's error unrecoverable, will rethrow
      Mockito.when(
              harness
                  .getContext()
                  .retryPolicy()
                  .onErrorResponse(eq(PREPARE_REQUEST), any(OverloadedException.class), eq(0)))
          .thenReturn(RetryDecision.IGNORE);

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareAsyncHandler(
                  PREPARE_REQUEST,
                  preparedStatementsCache,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      // Success on node2, reprepare on node3
      assertThat(prepareFuture)
          .isFailed(
              error -> {
                assertThat(error)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(
                        "IGNORE decisions are not allowed for prepare requests, "
                            + "please fix your retry policy.");
                node2Behavior.verifyNoWrite();
                node3Behavior.verifyNoWrite();
              });
    }
  }

  private static Frame defaultFrameOf(Message responseMessage) {
    return Frame.forResponse(
        DefaultProtocolVersion.V4.getCode(),
        0,
        null,
        Frame.NO_PAYLOAD,
        Collections.emptyList(),
        responseMessage);
  }

  private static Message simplePrepared() {
    RowsMetadata variablesMetadata =
        new RowsMetadata(
            ImmutableList.of(
                new ColumnSpec(
                    "ks",
                    "table",
                    "key",
                    0,
                    RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR))),
            null,
            new int[] {0},
            null);
    RowsMetadata resultMetadata =
        new RowsMetadata(
            ImmutableList.of(
                new ColumnSpec(
                    "ks",
                    "table",
                    "message",
                    0,
                    RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR))),
            null,
            new int[] {},
            null);
    return new Prepared(
        Bytes.fromHexString("0xffff").array(), null, variablesMetadata, resultMetadata);
  }

  private static void assertMatchesSimplePrepared(PreparedStatement statement) {
    assertThat(Bytes.toHexString(statement.getId())).isEqualTo("0xffff");

    ColumnDefinitions variableDefinitions = statement.getVariableDefinitions();
    assertThat(variableDefinitions).hasSize(1);
    assertThat(variableDefinitions.get(0).getName().asInternal()).isEqualTo("key");

    ColumnDefinitions resultSetDefinitions = statement.getResultSetDefinitions();
    assertThat(resultSetDefinitions).hasSize(1);
    assertThat(resultSetDefinitions.get(0).getName().asInternal()).isEqualTo("message");
  }
}
