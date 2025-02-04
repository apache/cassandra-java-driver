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

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlRequestHandler;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** TODO: how do we access the context */
public class OtelRequestTracker implements RequestTracker {
  //  private final OpenTelemetry openTelemetry;

  private final Map<String, TracingInfo> logPrefixToTracingInfoMap = new ConcurrentHashMap<>();

  private final Tracer tracer;

  private final Logger LOG = LoggerFactory.getLogger(OtelRequestTracker.class);

  private final ExecutorService threadPool;

  //  private Session session;
  private RequestLogFormatter formatter;

  public OtelRequestTracker(OpenTelemetry openTelemetry) {
    //    this.openTelemetry = openTelemetry;
    this.tracer =
        openTelemetry.getTracer("com.datastax.oss.driver.internal.core.tracker.OtelRequestTracker");
    this.threadPool =
        new ThreadPoolExecutor(
            1,
            Math.max(Runtime.getRuntime().availableProcessors(), 1),
            10,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1000),
            new ThreadFactoryBuilder().setNameFormat("otel-thread-%d").build(),
            new ThreadPoolExecutor.AbortPolicy());
  }

  @Override
  public void close() throws Exception {
    threadPool.shutdown();
    threadPool.awaitTermination(10, TimeUnit.SECONDS);
    logPrefixToTracingInfoMap.clear();
  }

  @Override
  public void onRequestCreated(
      @NonNull Request request,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull String requestLogPrefix) {
    Span parentSpan = tracer.spanBuilder("Cassandra Java Driver").startSpan();
    TracingInfo tracingInfo = new TracingInfo(parentSpan);
    logPrefixToTracingInfoMap.put(requestLogPrefix, tracingInfo);
    addRequestAttributesToSpan(request, parentSpan);
    LOG.debug("Request created: {}", requestLogPrefix);
  }

  @Override
  public void onRequestCreatedForNode(
      @NonNull Request request,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String requestLogPrefix) {

    logPrefixToTracingInfoMap.computeIfPresent(
        nodePrefixToRequestPrefix(requestLogPrefix),
        (k, v) -> {
          Span parentSpan = v.parentSpan;
          Span span =
              tracer
                  .spanBuilder("Cassandra Java Driver")
                  .setParent(Context.current().with(parentSpan))
                  .startSpan();
          addRequestAttributesToSpan(request, span);
          return v;
        });
    LOG.debug("Request created for node: {}", requestLogPrefix);
  }

  @Override
  public void onSuccess(
      long latencyNanos, @NonNull ExecutionInfo executionInfo, @NonNull String requestLogPrefix) {
    logPrefixToTracingInfoMap.computeIfPresent(
        requestLogPrefix,
        (k, v) -> {
          Span span = v.parentSpan;
          span.setStatus(StatusCode.OK);
          span.end();
          return null;
        });
  }

  @Override
  public void onError(
      long latencyNanos, @NonNull ExecutionInfo executionInfo, @NonNull String requestLogPrefix) {
    logPrefixToTracingInfoMap.computeIfPresent(
        requestLogPrefix,
        (k, v) -> {
          Span span = v.parentSpan;
          if (!executionInfo.getErrors().isEmpty()) {
            span.recordException(executionInfo.getErrors().get(0).getValue());
          }
          span.setStatus(StatusCode.ERROR);
          span.end();
          return null;
        });
  }

  @Override
  public void onNodeSuccess(
      long latencyNanos, @NonNull ExecutionInfo executionInfo, @NonNull String requestLogPrefix) {
    logPrefixToTracingInfoMap.computeIfPresent(
        nodePrefixToRequestPrefix(requestLogPrefix),
        (k, v) -> {
          Span span = v.parentSpan;
          span.setStatus(StatusCode.OK);
          span.end();
          return null;
        });
  }

  @Override
  public void onNodeError(
      long latencyNanos, @NonNull ExecutionInfo executionInfo, @NonNull String requestLogPrefix) {
    logPrefixToTracingInfoMap.computeIfPresent(
        nodePrefixToRequestPrefix(requestLogPrefix),
        (k, v) -> {
          Span span = v.parentSpan;
          if (!executionInfo.getErrors().isEmpty()) {
            span.recordException(executionInfo.getErrors().get(0).getValue());
          }
          span.setStatus(StatusCode.ERROR);
          span.end();
          return null;
        });
  }

  @Override
  public void onSessionReady(@NonNull Session session) {
    //    this.session = session;
    this.formatter = ((DefaultDriverContext) session.getContext()).getRequestLogFormatter();
  }

  private static class TracingInfo {
    private final Span parentSpan;

    private TracingInfo(Span parentSpan) {
      this.parentSpan = parentSpan;
    }
  }

  private void addRequestAttributesToSpan(Request request, Span span) {
    if (request.getKeyspace() != null)
      span.setAttribute("db.cassandra.keyspace", request.getKeyspace().asCql(true));
    span.setAttribute("db.query.text", statementToString(request));
    if (request.isIdempotent() != null)
      span.setAttribute("db.cassandra.idempotence", request.isIdempotent());
  }

  private String statementToString(Request request) {
    StringBuilder builder = new StringBuilder();
    assert this.formatter != null;
    this.formatter.appendQueryString(
        request, RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH, builder);
    this.formatter.appendValues(
        request,
        RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUES,
        RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUE_LENGTH,
        true,
        builder);
    return builder.toString();
  }

  /**
   * This depends on the implementation of {@link
   * CqlRequestHandler.NodeResponseCallback#NodeResponseCallback(Statement, Node, Queue,
   * DriverChannel, int, int, boolean, String) NodeResponseCallback}
   *
   * @param nodePrefix s0|1716164115|0
   * @return the request prefix, like s0|1716164115
   */
  private String nodePrefixToRequestPrefix(String nodePrefix) {
    int lastSeparatorIndex = nodePrefix.lastIndexOf("|");
    return nodePrefix.substring(0, lastSeparatorIndex);
  }
}
