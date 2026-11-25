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
import static com.datastax.oss.driver.internal.core.cql.CqlRequestHandlerTestBase.defaultFrameOf;
import static com.datastax.oss.driver.internal.core.cql.CqlRequestHandlerTrackerTest.execInfoMatcher;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.tracker.NoopRequestTracker;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

public class CqlPrepareHandlerTrackerTest extends CqlPrepareHandlerTestBase {

  @Test
  public void should_invoke_request_tracker() {
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            .withDefaultIdempotence(true)
            .withResponse(
                node1,
                defaultFrameOf(
                    new Error(ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING, "mock message")))
            .withResponse(node2, defaultFrameOf(CqlPrepareHandlerTest.simplePrepared()))
            .build()) {

      RequestTracker requestTracker = mock(RequestTracker.class);
      when(harness.getContext().getRequestTracker()).thenReturn(requestTracker);

      CompletionStage<PreparedStatement> resultSetFuture =
          new CqlPrepareHandler(PREPARE_REQUEST, harness.getSession(), harness.getContext(), "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(
              resultSet -> {
                verify(requestTracker)
                    .onRequestCreated(
                        eq(PREPARE_REQUEST), any(DriverExecutionProfile.class), any(String.class));
                verify(requestTracker)
                    .onRequestCreatedForNode(
                        eq(PREPARE_REQUEST),
                        any(DriverExecutionProfile.class),
                        eq(node1),
                        any(String.class));
                verify(requestTracker)
                    .onNodeError(
                        anyLong(),
                        argThat(
                            execInfoMatcher(node1, PREPARE_REQUEST, BootstrappingException.class)),
                        any(String.class));
                verify(requestTracker)
                    .onRequestCreatedForNode(
                        eq(PREPARE_REQUEST),
                        any(DriverExecutionProfile.class),
                        eq(node2),
                        any(String.class));
                verify(requestTracker)
                    .onNodeSuccess(
                        anyLong(),
                        argThat(execInfoMatcher(node2, PREPARE_REQUEST, null)),
                        any(String.class));
                verify(requestTracker)
                    .onSuccess(
                        anyLong(),
                        argThat(execInfoMatcher(node2, PREPARE_REQUEST, null)),
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
            .withResponse(node2, defaultFrameOf(CqlPrepareHandlerTest.simplePrepared()))
            .build()) {

      RequestTracker requestTracker = spy(new NoopRequestTracker(harness.getContext()));
      when(harness.getContext().getRequestTracker()).thenReturn(requestTracker);

      CompletionStage<PreparedStatement> resultSetFuture =
          new CqlPrepareHandler(PREPARE_REQUEST, harness.getSession(), harness.getContext(), "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(resultSet -> verifyNoMoreInteractions(requestTracker));
    }
  }
}
