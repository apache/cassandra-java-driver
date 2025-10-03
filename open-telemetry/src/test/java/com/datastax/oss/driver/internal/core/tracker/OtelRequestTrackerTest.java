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
package com.datastax.oss.driver.internal.core.tracker;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.TypesafeDriverConfig;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Strict.class)
public class OtelRequestTrackerTest {
  @Mock private OpenTelemetry openTelemetry;
  @Mock private Tracer tracer;
  @Mock private DriverExecutionProfile profile;
  @Mock private SpanBuilder sessionSpanBuilder;
  @Mock private SpanBuilder nodeSpanBuilder;
  @Mock private Span sessionSpan;
  @Mock private Span nodeSpan;
  @Mock private CqlSession session;
  @Mock private DefaultDriverContext context;
  private DriverConfig config =
      new TypesafeDriverConfig(DefaultDriverConfigLoader.DEFAULT_CONFIG_SUPPLIER.get());
  private RequestLogFormatter requestLogFormatter = new RequestLogFormatter(context);
  private String sessionLogPrefix = "s0|229540037";
  private String nodeLogPrefix = "s0|229540037|0";
  @Mock private Node node1;
  @Mock private ExecutionInfo executionInfo;
  private SimpleStatement statement = SimpleStatement.newInstance("SELECT * FROM test");

  @Before
  public void setup() {
    given(
            openTelemetry.getTracer(
                "com.datastax.oss.driver.internal.core.tracker.OtelRequestTracker"))
        .willReturn(tracer);
    given(tracer.spanBuilder("Cassandra Java Driver Session Request"))
        .willReturn(sessionSpanBuilder);
    given(tracer.spanBuilder("Cassandra Java Driver Node Request")).willReturn(nodeSpanBuilder);
    given(sessionSpanBuilder.startSpan()).willReturn(sessionSpan);
    given(nodeSpanBuilder.startSpan()).willReturn(nodeSpan);
    given(nodeSpanBuilder.setParent(Context.current().with(sessionSpan)))
        .willReturn(nodeSpanBuilder);
    given(session.getContext()).willReturn(context);
    given(context.getRequestLogFormatter()).willReturn(requestLogFormatter);
    given(context.getConfig()).willReturn(config);
    given(executionInfo.getRequest()).willReturn(statement);
  }

  @Test
  public void should_send_trace_on_request_created() {
    // Given
    OtelRequestTracker tracker = new OtelRequestTracker(openTelemetry);
    tracker.onSessionReady(session);
    // When
    tracker.onRequestCreated(statement, profile, sessionLogPrefix);
    // Then
    verify(tracer).spanBuilder("Cassandra Java Driver Session Request");
    verify(sessionSpan).setAttribute(AttributeKey.stringKey("db.query.text"), "SELECT * FROM test");

    // when
    tracker.onRequestCreatedForNode(statement, profile, node1, nodeLogPrefix);
    // Then
    verify(tracer).spanBuilder("Cassandra Java Driver Node Request");
    verify(nodeSpan).setAttribute(AttributeKey.stringKey("db.query.text"), "SELECT * FROM test");
  }

  @Test
  public void should_send_trace_on_success() {
    // Given
    OtelRequestTracker tracker = new OtelRequestTracker(openTelemetry);
    tracker.onSessionReady(session);
    tracker.onRequestCreated(statement, profile, sessionLogPrefix);
    tracker.onRequestCreatedForNode(statement, profile, node1, nodeLogPrefix);
    // When
    tracker.onNodeSuccess(1L, executionInfo, nodeLogPrefix);
    // Then
    verify(nodeSpan).end();
    verify(nodeSpan, times(2))
        .setAttribute(
            AttributeKey.stringKey("db.operation.name"), "Node_Request(DefaultSimpleStatement)");
    // When
    tracker.onSuccess(1L, executionInfo, sessionLogPrefix);
    // Then
    verify(sessionSpan).end();
    verify(sessionSpan, times(2))
        .setAttribute(
            AttributeKey.stringKey("db.operation.name"), "Session_Request(DefaultSimpleStatement)");
  }

  @Test
  public void should_send_trace_on_error() {
    // Given
    OtelRequestTracker tracker = new OtelRequestTracker(openTelemetry);
    tracker.onSessionReady(session);
    tracker.onRequestCreated(statement, profile, sessionLogPrefix);
    tracker.onRequestCreatedForNode(statement, profile, node1, nodeLogPrefix);
    // When
    tracker.onNodeError(1L, executionInfo, nodeLogPrefix);
    // Then
    verify(nodeSpan).end();
    // When
    tracker.onError(1L, executionInfo, sessionLogPrefix);
    // Then
    verify(sessionSpan).end();
  }
}
