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
package com.datastax.oss.driver.core.tracker;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.tracker.RequestLogFormatter;
import com.datastax.oss.driver.internal.core.tracker.RequestLogger;
import edu.umd.cs.findbugs.annotations.NonNull;

public class RequestNodeLoggerExample extends RequestLogger {

  public RequestNodeLoggerExample(DriverContext context) {
    super(new RequestLogFormatter(context));
  }

  @Override
  public void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    if (!executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED)) {
      return;
    }

    int maxQueryLength =
        executionProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH);
    boolean showValues = executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_VALUES);
    int maxValues =
        showValues ? executionProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES) : 0;
    int maxValueLength =
        showValues
            ? executionProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH)
            : 0;
    boolean showStackTraces =
        executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES);

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
  public void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    boolean successEnabled =
        executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED);
    boolean slowEnabled =
        executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED);
    if (!successEnabled && !slowEnabled) {
      return;
    }

    long slowThresholdNanos =
        executionProfile.isDefined(DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD)
            ? executionProfile
                .getDuration(DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD)
                .toNanos()
            : Long.MAX_VALUE;
    boolean isSlow = latencyNanos > slowThresholdNanos;
    if ((isSlow && !slowEnabled) || (!isSlow && !successEnabled)) {
      return;
    }

    int maxQueryLength =
        executionProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH);
    boolean showValues = executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_VALUES);
    int maxValues =
        showValues ? executionProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES) : 0;
    int maxValueLength =
        showValues
            ? executionProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH)
            : 0;

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
}
