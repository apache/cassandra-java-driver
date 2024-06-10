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
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A request tracker that logs the requests executed through the session, according to a set of
 * configurable options.
 *
 * <p>To activate this tracker, modify the {@code advanced.request-tracker} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.request-tracker {
 *     classes = [RequestLogger]
 *     logs {
 *       success { enabled = true }
 *       slow { enabled = true, threshold = 1 second }
 *       error { enabled = true }
 *       max-query-length = 500
 *       show-values = true
 *       max-value-length = 50
 *       max-values = 50
 *       show-stack-traces = true
 *     }
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 *
 * <p>Note that if a tracker is specified programmatically with {@link
 * SessionBuilder#addRequestTracker(RequestTracker)}, the configuration is ignored.
 */
@ThreadSafe
public class RequestLogger implements RequestTracker {

  private static final Logger LOG = LoggerFactory.getLogger(RequestLogger.class);

  public static final int DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH = 500;
  public static final boolean DEFAULT_REQUEST_LOGGER_SHOW_VALUES = true;
  public static final int DEFAULT_REQUEST_LOGGER_MAX_VALUES = 50;
  public static final int DEFAULT_REQUEST_LOGGER_MAX_VALUE_LENGTH = 50;

  private final RequestLogFormatter formatter;

  public RequestLogger(DriverContext context) {
    this(new RequestLogFormatter(context));
  }

  protected RequestLogger(RequestLogFormatter formatter) {
    this.formatter = formatter;
  }

  @Override
  public void onSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {

    boolean successEnabled =
        executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED, false);
    boolean slowEnabled =
        executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED, false);
    if (!successEnabled && !slowEnabled) {
      return;
    }

    long slowThresholdNanos =
        executionProfile
            .getDuration(DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD, Duration.ofSeconds(1))
            .toNanos();
    boolean isSlow = latencyNanos > slowThresholdNanos;
    if ((isSlow && !slowEnabled) || (!isSlow && !successEnabled)) {
      return;
    }

    int maxQueryLength =
        executionProfile.getInt(
            DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
            DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH);
    boolean showValues =
        executionProfile.getBoolean(
            DefaultDriverOption.REQUEST_LOGGER_VALUES, DEFAULT_REQUEST_LOGGER_SHOW_VALUES);
    int maxValues =
        executionProfile.getInt(
            DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES, DEFAULT_REQUEST_LOGGER_MAX_VALUES);
    int maxValueLength =
        executionProfile.getInt(
            DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH,
            DEFAULT_REQUEST_LOGGER_MAX_VALUE_LENGTH);

    logSuccess(
        request,
        latencyNanos,
        isSlow,
        node,
        maxQueryLength,
        showValues,
        maxValues,
        maxValueLength,
        logPrefix);
  }

  @Override
  public void onError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      Node node,
      @NonNull String logPrefix) {

    if (!executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED, false)) {
      return;
    }

    int maxQueryLength =
        executionProfile.getInt(
            DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
            DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH);
    boolean showValues =
        executionProfile.getBoolean(
            DefaultDriverOption.REQUEST_LOGGER_VALUES, DEFAULT_REQUEST_LOGGER_SHOW_VALUES);
    int maxValues =
        executionProfile.getInt(
            DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES, DEFAULT_REQUEST_LOGGER_MAX_VALUES);

    int maxValueLength =
        executionProfile.getInt(
            DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH,
            DEFAULT_REQUEST_LOGGER_MAX_VALUE_LENGTH);
    boolean showStackTraces =
        executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES, false);

    logError(
        request,
        error,
        latencyNanos,
        node,
        maxQueryLength,
        showValues,
        maxValues,
        maxValueLength,
        showStackTraces,
        logPrefix);
  }

  @Override
  public void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    // Nothing to do
  }

  @Override
  public void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    // Nothing to do
  }

  @Override
  public void close() throws Exception {
    // nothing to do
  }

  protected void logSuccess(
      Request request,
      long latencyNanos,
      boolean isSlow,
      Node node,
      int maxQueryLength,
      boolean showValues,
      int maxValues,
      int maxValueLength,
      String logPrefix) {

    StringBuilder builder = formatter.logBuilder(logPrefix, node);
    if (isSlow) {
      formatter.appendSlowDescription(builder);
    } else {
      formatter.appendSuccessDescription(builder);
    }
    formatter.appendLatency(latencyNanos, builder);
    formatter.appendRequest(
        request, maxQueryLength, showValues, maxValues, maxValueLength, builder);
    LOG.info(builder.toString());
  }

  protected void logError(
      Request request,
      Throwable error,
      long latencyNanos,
      Node node,
      int maxQueryLength,
      boolean showValues,
      int maxValues,
      int maxValueLength,
      boolean showStackTraces,
      String logPrefix) {

    StringBuilder builder = formatter.logBuilder(logPrefix, node);
    formatter.appendErrorDescription(builder);
    formatter.appendLatency(latencyNanos, builder);
    formatter.appendRequest(
        request, maxQueryLength, showValues, maxValues, maxValueLength, builder);
    if (showStackTraces) {
      LOG.error(builder.toString(), error);
    } else {
      LOG.error("{} [{}]", builder.toString(), error.toString());
    }
  }
}
