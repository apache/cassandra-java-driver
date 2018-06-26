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
package com.datastax.oss.driver.api.core.tracker;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.tracker.RequestLogFormatter;
import com.datastax.oss.driver.internal.core.tracker.RequestLogger;
import edu.umd.cs.findbugs.annotations.NonNull;

public class RequestNodeLoggerExample extends RequestLogger {

  public RequestNodeLoggerExample(DriverContext context) {
    super(context.sessionName(), new RequestLogFormatter(context));
  }

  @Override
  public void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverConfigProfile configProfile,
      @NonNull Node node) {
    if (!configProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED)) {
      return;
    }

    int maxQueryLength = configProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH);
    boolean showValues = configProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_VALUES);
    int maxValues =
        showValues ? configProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES) : 0;
    int maxValueLength =
        showValues ? configProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH) : 0;
    boolean showStackTraces =
        configProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES);

    logError(
        request,
        error,
        latencyNanos,
        node,
        maxQueryLength,
        showValues,
        maxValues,
        maxValueLength,
        showStackTraces);
  }

  @Override
  public void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverConfigProfile configProfile,
      @NonNull Node node) {
    boolean successEnabled =
        configProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED);
    boolean slowEnabled = configProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED);
    if (!successEnabled && !slowEnabled) {
      return;
    }

    long slowThresholdNanos =
        configProfile.isDefined(DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD)
            ? configProfile.getDuration(DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD).toNanos()
            : Long.MAX_VALUE;
    boolean isSlow = latencyNanos > slowThresholdNanos;
    if ((isSlow && !slowEnabled) || (!isSlow && !successEnabled)) {
      return;
    }

    int maxQueryLength = configProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH);
    boolean showValues = configProfile.getBoolean(DefaultDriverOption.REQUEST_LOGGER_VALUES);
    int maxValues =
        showValues ? configProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES) : 0;
    int maxValueLength =
        showValues ? configProfile.getInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH) : 0;

    logSuccess(
        request, latencyNanos, isSlow, node, maxQueryLength, showValues, maxValues, maxValueLength);
  }
}
