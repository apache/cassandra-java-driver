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
package com.datastax.oss.driver.internal.core.cql;

import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import com.datastax.oss.driver.internal.core.tracker.NoopRequestTracker;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.Invocation;

public class CqlRequestHandlerTrackerTest extends CqlRequestHandlerTestBase {
  private static final String ON_REQUEST_CREATED = "onRequestCreated";
  private static final String ON_REQUEST_CREATED_FOR_NODE = "onRequestCreatedForNode";
  private static final String ON_NODE_SUCCESS = "onNodeSuccess";
  private static final String ON_NODE_ERROR = "onNodeError";
  private static final String ON_SUCCESS = "onSuccess";
  private static final Pattern LOG_PREFIX_PER_REQUEST = Pattern.compile("(test)\\|\\d*");
  private static final Pattern LOG_PREFIX_WITH_EXECUTION_NUMBER =
      Pattern.compile("(test)\\|\\d*\\|\\d*");

  @Test
  public void should_invoke_request_tracker() {
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            .withDefaultIdempotence(true)
            .withResponse(
                node1,
                defaultFrameOf(
                    new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")))
            .withResponse(node2, defaultFrameOf(singleRow()))
            .build()) {

      RequestTracker requestTracker = mock(RequestTracker.class);
      when(harness.getContext().getRequestTracker()).thenReturn(requestTracker);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(
              resultSet -> {
                verify(requestTracker)
                    .onRequestCreated(
                        eq(UNDEFINED_IDEMPOTENCE_STATEMENT),
                        any(DriverExecutionProfile.class),
                        any(String.class));
                verify(requestTracker)
                    .onRequestCreatedForNode(
                        eq(UNDEFINED_IDEMPOTENCE_STATEMENT),
                        any(DriverExecutionProfile.class),
                        eq(node1),
                        any(String.class));
                verify(requestTracker)
                    .onNodeError(
                        anyLong(),
                        argThat(
                            execInfoMatcher(
                                node1,
                                UNDEFINED_IDEMPOTENCE_STATEMENT,
                                BootstrappingException.class)),
                        any(String.class));
                verify(requestTracker)
                    .onRequestCreatedForNode(
                        eq(UNDEFINED_IDEMPOTENCE_STATEMENT),
                        any(DriverExecutionProfile.class),
                        eq(node2),
                        any(String.class));
                verify(requestTracker)
                    .onNodeSuccess(
                        anyLong(),
                        argThat(execInfoMatcher(node2, UNDEFINED_IDEMPOTENCE_STATEMENT, null)),
                        any(String.class));
                verify(requestTracker)
                    .onSuccess(
                        anyLong(),
                        argThat(execInfoMatcher(node2, UNDEFINED_IDEMPOTENCE_STATEMENT, null)),
                        any(String.class));
                verifyNoMoreInteractions(requestTracker);
              });
    }
  }

  @Test
  public void should_not_invoke_noop_request_tracker() {
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            .withDefaultIdempotence(true)
            .withResponse(
                node1,
                defaultFrameOf(
                    new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")))
            .withResponse(node2, defaultFrameOf(singleRow()))
            .build()) {

      RequestTracker requestTracker = spy(new NoopRequestTracker(harness.getContext()));
      when(harness.getContext().getRequestTracker()).thenReturn(requestTracker);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(resultSet -> verifyNoMoreInteractions(requestTracker));
    }
  }

  @Test
  public void should_invoke_implicit_prepare_request_tracker() {
    ByteBuffer mockId = Bytes.fromHexString("0xffff");

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(preparedStatement.getId()).thenReturn(mockId);
    ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
    when(columnDefinitions.size()).thenReturn(0);
    when(preparedStatement.getResultSetDefinitions()).thenReturn(columnDefinitions);
    BoundStatement boundStatement = mock(BoundStatement.class);
    when(boundStatement.getPreparedStatement()).thenReturn(preparedStatement);
    when(boundStatement.getValues()).thenReturn(Collections.emptyList());
    when(boundStatement.getNowInSeconds()).thenReturn(Statement.NO_NOW_IN_SECONDS);

    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    // For the first attempt that gets the UNPREPARED response
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    // For the second attempt that succeeds
    harnessBuilder.withResponse(node1, defaultFrameOf(singleRow()));

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      RequestTracker requestTracker = mock(RequestTracker.class);
      when(harness.getContext().getRequestTracker()).thenReturn(requestTracker);

      // The handler will look for the info to reprepare in the session's cache, put it there
      ConcurrentMap<ByteBuffer, RepreparePayload> repreparePayloads = new ConcurrentHashMap<>();
      repreparePayloads.put(
          mockId, new RepreparePayload(mockId, "mock query", null, Collections.emptyMap()));
      when(harness.getSession().getRepreparePayloads()).thenReturn(repreparePayloads);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      // Before we proceed, mock the PREPARE exchange that will occur as soon as we complete the
      // first response.
      node1Behavior.mockFollowupRequest(
          Prepare.class, defaultFrameOf(new Prepared(Bytes.getArray(mockId), null, null, null)));

      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(
          defaultFrameOf(new Unprepared("mock message", Bytes.getArray(mockId))));

      assertThatStage(resultSetFuture)
          .isSuccess(
              resultSet -> {
                List<Invocation> invocations =
                    (List<Invocation>) mockingDetails(requestTracker).getInvocations();
                assertThat(invocations).hasSize(10);
                // start processing CQL statement
                checkOnCreateInvocation(
                    invocations.get(0),
                    ON_REQUEST_CREATED,
                    DefaultSimpleStatement.class,
                    LOG_PREFIX_PER_REQUEST);
                checkOnCreateInvocation(
                    invocations.get(1),
                    ON_REQUEST_CREATED_FOR_NODE,
                    DefaultSimpleStatement.class,
                    LOG_PREFIX_WITH_EXECUTION_NUMBER);
                checkOnEndInvocation(
                    invocations.get(2),
                    ON_NODE_ERROR,
                    DefaultSimpleStatement.class,
                    LOG_PREFIX_WITH_EXECUTION_NUMBER);
                // implicit reprepare statement
                checkOnCreateInvocation(
                    invocations.get(3),
                    ON_REQUEST_CREATED,
                    DefaultPrepareRequest.class,
                    LOG_PREFIX_WITH_EXECUTION_NUMBER);
                checkOnCreateInvocation(
                    invocations.get(4),
                    ON_REQUEST_CREATED_FOR_NODE,
                    DefaultPrepareRequest.class,
                    LOG_PREFIX_WITH_EXECUTION_NUMBER);
                checkOnEndInvocation(
                    invocations.get(5),
                    ON_NODE_SUCCESS,
                    DefaultPrepareRequest.class,
                    LOG_PREFIX_WITH_EXECUTION_NUMBER);
                checkOnEndInvocation(
                    invocations.get(6),
                    ON_SUCCESS,
                    DefaultPrepareRequest.class,
                    LOG_PREFIX_WITH_EXECUTION_NUMBER);
                // send new statement and process it
                checkOnCreateInvocation(
                    invocations.get(7),
                    ON_REQUEST_CREATED_FOR_NODE,
                    DefaultSimpleStatement.class,
                    LOG_PREFIX_WITH_EXECUTION_NUMBER);
                checkOnEndInvocation(
                    invocations.get(8),
                    ON_NODE_SUCCESS,
                    DefaultSimpleStatement.class,
                    LOG_PREFIX_WITH_EXECUTION_NUMBER);
                checkOnEndInvocation(
                    invocations.get(9),
                    ON_SUCCESS,
                    DefaultSimpleStatement.class,
                    LOG_PREFIX_PER_REQUEST);
              });
    }
  }

  private void checkOnCreateInvocation(
      Invocation invocation, String methodName, Class<?> firstParameter, Pattern logPrefixPattern) {
    assertThat(invocation.getMethod().getName()).isEqualTo(methodName);
    assertThat(invocation.getArguments()[0]).isInstanceOf(firstParameter);
    String logPrefix = invocation.getArguments()[invocation.getArguments().length - 1].toString();
    assertThat(logPrefix).matches(logPrefixPattern);
  }

  private void checkOnEndInvocation(
      Invocation invocation, String methodName, Class<?> firstParameter, Pattern logPrefixPattern) {
    assertThat(invocation.getMethod().getName()).isEqualTo(methodName);
    assertThat(((ExecutionInfo) invocation.getArguments()[1]).getRequest())
        .isInstanceOf(firstParameter);
    String logPrefix = invocation.getArguments()[invocation.getArguments().length - 1].toString();
    assertThat(logPrefix).matches(logPrefixPattern);
  }

  public static ArgumentMatcher<ExecutionInfo> execInfoMatcher(
      Node node, Request request, Class<? extends Throwable> errorClass) {
    return executionInfo ->
        node.equals(executionInfo.getCoordinator())
            && request.equals(executionInfo.getRequest())
            && (errorClass != null
                ? executionInfo.getDriverError() != null
                    && executionInfo.getDriverError().getClass().isAssignableFrom(errorClass)
                : executionInfo.getDriverError() == null)
            && executionInfo.getExecutionProfile() != null;
  }
}
