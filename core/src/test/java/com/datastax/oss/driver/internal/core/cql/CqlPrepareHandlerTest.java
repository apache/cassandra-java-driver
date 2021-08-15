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
import static com.datastax.oss.driver.internal.core.cql.CqlRequestHandlerTestBase.defaultFrameOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.NodeUnavailableException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CqlPrepareHandlerTest {

  private static final DefaultPrepareRequest PREPARE_REQUEST =
      new DefaultPrepareRequest("mock query");

  @Mock private Node node1;
  @Mock private Node node2;
  @Mock private Node node3;

  private final Map<String, ByteBuffer> payload =
      ImmutableMap.of("key1", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void should_prepare_on_first_node_and_reprepare_on_others() {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareHandler(PREPARE_REQUEST, harness.getSession(), harness.getContext(), "test")
              .handle();

      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      // The future waits for the reprepare attempt on other nodes, so it's not done yet.
      assertThatStage(prepareFuture).isNotDone();

      // Should now reprepare on the remaining nodes:
      node2Behavior.verifyWrite();
      node2Behavior.setWriteSuccess();
      node2Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      node3Behavior.verifyWrite();
      node3Behavior.setWriteSuccess();
      node3Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      assertThatStage(prepareFuture).isSuccess(CqlPrepareHandlerTest::assertMatchesSimplePrepared);
    }
  }

  @Test
  public void should_not_reprepare_on_other_nodes_if_disabled_in_config() {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      DriverExecutionProfile config = harness.getContext().getConfig().getDefaultProfile();
      when(config.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES)).thenReturn(false);

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareHandler(PREPARE_REQUEST, harness.getSession(), harness.getContext(), "test")
              .handle();

      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      // The future should complete immediately:
      assertThatStage(prepareFuture).isSuccess();

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
          new CqlPrepareHandler(PREPARE_REQUEST, harness.getSession(), harness.getContext(), "test")
              .handle();

      assertThatStage(prepareFuture).isNotDone();

      // Other nodes fail, the future should still succeed when all done
      node2Behavior.verifyWrite();
      node2Behavior.setWriteSuccess();
      node2Behavior.setResponseSuccess(
          defaultFrameOf(new Error(ProtocolConstants.ErrorCode.SERVER_ERROR, "mock error")));

      node3Behavior.verifyWrite();
      node3Behavior.setWriteFailure(new RuntimeException("mock error"));

      assertThatStage(prepareFuture).isSuccess(CqlPrepareHandlerTest::assertMatchesSimplePrepared);
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
      when(harness
              .getContext()
              .getRetryPolicy(anyString())
              .onErrorResponseVerdict(eq(PREPARE_REQUEST), any(OverloadedException.class), eq(0)))
          .thenReturn(RetryVerdict.RETRY_NEXT);

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareHandler(PREPARE_REQUEST, harness.getSession(), harness.getContext(), "test")
              .handle();

      // Success on node2, reprepare on node3
      assertThatStage(prepareFuture).isNotDone();
      node3Behavior.verifyWrite();
      node3Behavior.setWriteSuccess();
      node3Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));

      assertThatStage(prepareFuture).isSuccess(CqlPrepareHandlerTest::assertMatchesSimplePrepared);
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
      when(harness
              .getContext()
              .getRetryPolicy(anyString())
              .onErrorResponseVerdict(eq(PREPARE_REQUEST), any(OverloadedException.class), eq(0)))
          .thenReturn(RetryVerdict.RETHROW);

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareHandler(PREPARE_REQUEST, harness.getSession(), harness.getContext(), "test")
              .handle();

      // Success on node2, reprepare on node3
      assertThatStage(prepareFuture)
          .isFailed(
              error -> {
                assertThat(error).isInstanceOf(OverloadedException.class);
                node2Behavior.verifyNoWrite();
                node3Behavior.verifyNoWrite();
              });
    }
  }

  @Test
  public void should_fail_if_nodes_unavailable() {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    try (RequestHandlerTestHarness harness =
        harnessBuilder.withEmptyPool(node1).withEmptyPool(node2).build()) {
      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareHandler(PREPARE_REQUEST, harness.getSession(), harness.getContext(), "test")
              .handle();
      assertThatStage(prepareFuture)
          .isFailed(
              error -> {
                assertThat(error).isInstanceOf(AllNodesFailedException.class);
                Map<Node, List<Throwable>> allErrors =
                    ((AllNodesFailedException) error).getAllErrors();
                assertThat(allErrors).hasSize(2);
                assertThat(allErrors)
                    .hasEntrySatisfying(
                        node1,
                        nodeErrors ->
                            assertThat(nodeErrors)
                                .singleElement()
                                .isInstanceOf(NodeUnavailableException.class));
                assertThat(allErrors)
                    .hasEntrySatisfying(
                        node2,
                        nodeErrors ->
                            assertThat(nodeErrors)
                                .singleElement()
                                .isInstanceOf(NodeUnavailableException.class));
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
      RetryPolicy mockRetryPolicy =
          harness.getContext().getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME);
      when(mockRetryPolicy.onErrorResponseVerdict(
              eq(PREPARE_REQUEST), any(OverloadedException.class), eq(0)))
          .thenReturn(RetryVerdict.IGNORE);

      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareHandler(PREPARE_REQUEST, harness.getSession(), harness.getContext(), "test")
              .handle();

      // Success on node2, reprepare on node3
      assertThatStage(prepareFuture)
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

  @Test
  public void should_propagate_custom_payload_on_single_node() {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    DefaultPrepareRequest prepareRequest =
        new DefaultPrepareRequest(
            SimpleStatement.newInstance("irrelevant").setCustomPayload(payload));
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);
    node1Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));
    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      DriverExecutionProfile config = harness.getContext().getConfig().getDefaultProfile();
      when(config.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES)).thenReturn(false);
      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareHandler(prepareRequest, harness.getSession(), harness.getContext(), "test")
              .handle();
      verify(node1Behavior.channel)
          .write(any(Prepare.class), anyBoolean(), eq(payload), any(ResponseCallback.class));
      node2Behavior.verifyNoWrite();
      node3Behavior.verifyNoWrite();
      assertThatStage(prepareFuture).isSuccess(CqlPrepareHandlerTest::assertMatchesSimplePrepared);
    }
  }

  @Test
  public void should_propagate_custom_payload_on_all_nodes() {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    DefaultPrepareRequest prepareRequest =
        new DefaultPrepareRequest(
            SimpleStatement.newInstance("irrelevant").setCustomPayload(payload));
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);
    PoolBehavior node3Behavior = harnessBuilder.customBehavior(node3);
    node1Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));
    node2Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));
    node3Behavior.setResponseSuccess(defaultFrameOf(simplePrepared()));
    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      DriverExecutionProfile config = harness.getContext().getConfig().getDefaultProfile();
      when(config.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES)).thenReturn(true);
      CompletionStage<PreparedStatement> prepareFuture =
          new CqlPrepareHandler(prepareRequest, harness.getSession(), harness.getContext(), "test")
              .handle();
      verify(node1Behavior.channel)
          .write(any(Prepare.class), anyBoolean(), eq(payload), any(ResponseCallback.class));
      verify(node2Behavior.channel)
          .write(any(Prepare.class), anyBoolean(), eq(payload), any(ResponseCallback.class));
      verify(node3Behavior.channel)
          .write(any(Prepare.class), anyBoolean(), eq(payload), any(ResponseCallback.class));
      assertThatStage(prepareFuture).isSuccess(CqlPrepareHandlerTest::assertMatchesSimplePrepared);
    }
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
