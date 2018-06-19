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

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;

/** Tracks request execution for a session. */
public interface RequestTracker extends AutoCloseable {

  /**
   * Invoked each time a request succeeds.
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the result is made available to the client).
   * @param configProfile the configuration profile that this request was executed with.
   * @param node the node that returned the successful response.
   */
  void onSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverConfigProfile configProfile,
      @NonNull Node node);

  /**
   * Invoked each time a request fails.
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the error is propagated to the client).
   * @param configProfile the configuration profile that this request was executed with.
   * @param node the node that returned the error response, or {@code null} if the error occurred
   */
  void onError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverConfigProfile configProfile,
      Node node);

  /**
   * Invoked each time a request fails at the node level. Similar to onError but at a per node
   * level.
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the error is propagated to the client).
   * @param configProfile the configuration profile that this request was executed with.
   * @param node the node that returned the error response, or {@code null} if the error occurred
   */
  void onNodeError(
      Request request,
      Throwable error,
      long latencyNanos,
      DriverConfigProfile configProfile,
      Node node);

  /**
   * Invoked each time a request succeeds at the node level. Similar to on Success but at per Node
   * level.
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the result is made available to the client).
   * @param configProfile the configuration profile that this request was executed with.
   * @param node the node that returned the successful response.
   */
  void onNodeSuccess(
      Request request, long latencyNanos, DriverConfigProfile configProfile, Node node);
}
