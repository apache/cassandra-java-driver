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

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Tracks request execution for a session.
 *
 * <p>There is exactly one tracker per {@link Session}. It can be provided either via the
 * configuration (see {@code reference.conf} in the manual or core driver JAR), or programmatically
 * via {@link SessionBuilder#withRequestTracker(RequestTracker)}.
 */
public interface RequestTracker extends AutoCloseable {

  /**
   * Invoked each time a request succeeds.
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the result is made available to the client).
   * @param executionProfile the execution profile of this request.
   * @param node the node that returned the successful response.
   */
  default void onSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node) {}

  /**
   * Invoked each time a request fails.
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the error is propagated to the client).
   * @param executionProfile the execution profile of this request.
   * @param node the node that returned the error response, or {@code null} if the error occurred
   */
  default void onError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @Nullable Node node) {}

  /**
   * Invoked each time a request fails at the node level. Similar to {@link #onError(Request,
   * Throwable, long, DriverExecutionProfile, Node)} but at a per node level.
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the error is propagated to the client).
   * @param executionProfile the execution profile of this request.
   * @param node the node that returned the error response.
   */
  default void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node) {}

  /**
   * Invoked each time a request succeeds at the node level. Similar to {@link #onSuccess(Request,
   * long, DriverExecutionProfile, Node)} but at per Node level.
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the result is made available to the client).
   * @param executionProfile the execution profile of this request.
   * @param node the node that returned the successful response.
   */
  default void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node) {}
}
