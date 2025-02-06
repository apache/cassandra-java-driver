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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlRequestHandler;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.SniEndPoint;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtelRequestTracker implements RequestTracker {

  private final Map<String, TracingInfo> logPrefixToTracingInfoMap = new ConcurrentHashMap<>();

  private final Tracer tracer;

  private final Logger LOG = LoggerFactory.getLogger(OtelRequestTracker.class);

  private final ExecutorService threadPool;

  private RequestLogFormatter formatter;
  private DefaultDriverContext context;
  private final Field proxyAddressField = getProxyAddressField();

  /**
   * Attributes that are "conditionally required" or "recommended" but we cannot provide: 1.
   * db.collection.name 2. db.response.status_code
   */
  private static final AttributeKey<String> DB_SYSTEM_NAME =
      AttributeKey.stringKey("db.system.name");

  private static final AttributeKey<String> DB_NAMESPACE = AttributeKey.stringKey("db.namespace");
  private static final AttributeKey<String> DB_OPERATION_NAME =
      AttributeKey.stringKey("db.operation.name");
  private static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");
  private static final AttributeKey<Long> SERVER_PORT = AttributeKey.longKey("server.port");
  private static final AttributeKey<String> CASSANDRA_CONSISTENCY_LEVEL =
      AttributeKey.stringKey("cassandra.consistency.level");
  private static final AttributeKey<String> CASSANDRA_COORDINATOR_DC =
      AttributeKey.stringKey("cassandra.coordinator.dc");
  private static final AttributeKey<String> CASSANDRA_COORDINATOR_ID =
      AttributeKey.stringKey("cassandra.coordinator.id");
  private static final AttributeKey<Long> CASSANDRA_PAGE_SIZE =
      AttributeKey.longKey("cassandra.page.size");
  private static final AttributeKey<Boolean> CASSANDRA_QUERY_IDEMPOTENT =
      AttributeKey.booleanKey("cassandra.query.idempotent");
  private static final AttributeKey<Long> CASSANDRA_SPECULATIVE_EXECUTION_COUNT =
      AttributeKey.longKey("cassandra.speculative_execution.count");
  private static final AttributeKey<Long> DB_OPERATION_BATCH_SIZE =
      AttributeKey.longKey("db.operation.batch.size");
  private static final AttributeKey<String> DB_QUERY_TEXT = AttributeKey.stringKey("db.query.text");
  private static final AttributeKey<String> SERVER_ADDRESS =
      AttributeKey.stringKey("server.address");

  public OtelRequestTracker(OpenTelemetry openTelemetry) {
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
    Span parentSpan = tracer.spanBuilder("Cassandra Java Driver Session Request").startSpan();
    TracingInfo tracingInfo = new TracingInfo(parentSpan);
    logPrefixToTracingInfoMap.put(requestLogPrefix, tracingInfo);
    addRequestAttributesToSpan(request, parentSpan, false);
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
                  .spanBuilder("Cassandra Java Driver Node Request")
                  .setParent(Context.current().with(parentSpan))
                  .startSpan();
          addRequestAttributesToSpan(request, span, true);
          v.addNodeSpan(requestLogPrefix, span);
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
          addRequestAttributesToSpan(executionInfo.getRequest(), span, false);
          addExecutionInfoToSpan(executionInfo, span);
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
          addRequestAttributesToSpan(executionInfo.getRequest(), span, false);
          addExecutionInfoToSpan(executionInfo, span);
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
          Span span = v.getNodeSpan(requestLogPrefix);
          span.setStatus(StatusCode.OK);
          addRequestAttributesToSpan(executionInfo.getRequest(), span, true);
          addExecutionInfoToSpan(executionInfo, span);
          span.end();
          return v;
        });
  }

  @Override
  public void onNodeError(
      long latencyNanos, @NonNull ExecutionInfo executionInfo, @NonNull String requestLogPrefix) {
    logPrefixToTracingInfoMap.computeIfPresent(
        nodePrefixToRequestPrefix(requestLogPrefix),
        (k, v) -> {
          Span span = v.getNodeSpan(requestLogPrefix);
          if (!executionInfo.getErrors().isEmpty()) {
            /*
             Find the first error for this node. Because a node can appear twice in the errors list due
             to retry policy, the error for this NodeRequest may not actually be the first error in
             this list. In that scenario, the wrong error may be attached to this span, but this is the best we can do.
            */
            executionInfo
                .getErrors()
                .forEach(
                    entry -> {
                      if (entry
                          .getKey()
                          .getHostId()
                          .equals(executionInfo.getCoordinator().getHostId())) {
                        span.recordException(entry.getValue());
                      }
                    });
          }
          span.setStatus(StatusCode.ERROR);
          addRequestAttributesToSpan(executionInfo.getRequest(), span, true);
          addExecutionInfoToSpan(executionInfo, span);
          span.end();
          return v;
        });
  }

  @Override
  public void onSessionReady(@NonNull Session session) {
    this.context = (DefaultDriverContext) session.getContext();
    this.formatter = this.context.getRequestLogFormatter();
  }

  private static class TracingInfo {
    private final Span parentSpan;
    private final Map<String, Span> nodeSpans = new ConcurrentHashMap<>(); // logPrefix -> span

    private TracingInfo(Span parentSpan) {
      this.parentSpan = parentSpan;
    }

    private void addNodeSpan(String logPrefix, Span span) {
      nodeSpans.put(logPrefix, span);
    }

    private Span getNodeSpan(String logPrefix) {
      return nodeSpans.get(logPrefix);
    }
  }

  private void addRequestAttributesToSpan(Request request, Span span, boolean isNodeRequest) {
    span.setAttribute(DB_SYSTEM_NAME, "cassandra");
    String operationName =
        String.format(
            "%s(%s)",
            isNodeRequest ? "Node_Request" : "Session_Request", request.getClass().getSimpleName());
    span.setAttribute(DB_OPERATION_NAME, operationName);
    if (request.getKeyspace() != null)
      span.setAttribute(DB_NAMESPACE, request.getKeyspace().asCql(true));

    if (request instanceof Statement<?>) {
      String consistencyLevel;
      if (((Statement<?>) request).getConsistencyLevel() != null) {
        consistencyLevel = ((Statement<?>) request).getConsistencyLevel().name();
      } else {
        consistencyLevel =
            context
                .getConfig()
                .getDefaultProfile()
                .getString(DefaultDriverOption.REQUEST_CONSISTENCY);
      }
      span.setAttribute(CASSANDRA_CONSISTENCY_LEVEL, consistencyLevel);

      int pageSize;
      if (((Statement<?>) request).getPageSize() > 0) {
        pageSize = ((Statement<?>) request).getPageSize();
      } else {
        pageSize =
            context.getConfig().getDefaultProfile().getInt(DefaultDriverOption.REQUEST_PAGE_SIZE);
      }
      span.setAttribute(CASSANDRA_PAGE_SIZE, pageSize);
    }

    span.setAttribute(DB_QUERY_TEXT, requestToString(request));
    if (request.isIdempotent() != null)
      span.setAttribute(CASSANDRA_QUERY_IDEMPOTENT, request.isIdempotent());

    if (request instanceof BatchStatement) {
      span.setAttribute(DB_OPERATION_BATCH_SIZE, ((BatchStatement) request).size());
    }
  }

  private void addExecutionInfoToSpan(ExecutionInfo executionInfo, Span span) {
    Node node = executionInfo.getCoordinator();
    if (node != null) {
      addServerAddressAndPortToSpan(span, node);
      span.setAttribute(CASSANDRA_COORDINATOR_ID, node.getHostId().toString());
      span.setAttribute(CASSANDRA_COORDINATOR_DC, node.getDatacenter());
    }

    /*
     Find the first error for this node. Because a node can appear twice in the errors list due
     to retry policy, the error for this NodeRequest may not actually be the first error in
     this list. In that scenario, the wrong error may be attached to this span, but this is the best we can do.
    */
    executionInfo
        .getErrors()
        .forEach(
            entry -> {
              if (entry.getKey().getHostId().equals(node.getHostId())) {
                span.setAttribute(ERROR_TYPE, entry.getValue().getClass().getSimpleName());
              }
            });

    span.setAttribute(
        CASSANDRA_SPECULATIVE_EXECUTION_COUNT, executionInfo.getSpeculativeExecutionCount());
  }

  private String requestToString(Request request) {
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

  private void addServerAddressAndPortToSpan(Span span, Node coordinator) {
    EndPoint endPoint = coordinator.getEndPoint();
    if (endPoint instanceof DefaultEndPoint) {
      InetSocketAddress address = ((DefaultEndPoint) endPoint).resolve();
      span.setAttribute(SERVER_ADDRESS, address.getHostString());
      span.setAttribute(SERVER_PORT, address.getPort());
    } else if (endPoint instanceof SniEndPoint && proxyAddressField != null) {
      SniEndPoint sniEndPoint = (SniEndPoint) endPoint;
      Object object = null;
      try {
        object = proxyAddressField.get(sniEndPoint);
      } catch (Exception e) {
        this.LOG.trace(
            "Error when accessing the private field proxyAddress of SniEndPoint using reflection.");
      }
      if (object instanceof InetSocketAddress) {
        InetSocketAddress address = (InetSocketAddress) object;
        span.setAttribute(SERVER_ADDRESS, address.getHostString());
        span.setAttribute(SERVER_PORT, address.getPort());
      }
    }
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

  @Nullable
  private Field getProxyAddressField() {
    try {
      Field field = SniEndPoint.class.getDeclaredField("proxyAddress");
      field.setAccessible(true);
      return field;
    } catch (Exception e) {
      return null;
    }
  }
}
