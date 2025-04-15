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
 * <p>Implementations of this interface can be registered either via the configuration (see {@code
 * reference.conf} in the manual or core driver JAR), or programmatically via {@link
 * SessionBuilder#addRequestTracker(RequestTracker)}.
 */
public interface RequestTracker extends AutoCloseable {

  /**
   * @deprecated This method only exists for backward compatibility. Override {@link
   *     #onSuccess(Request, long, DriverExecutionProfile, Node, String)} instead.
   */
  @Deprecated
  default void onSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node) {}

  /**
   * Invoked each time a session request succeeds. A session request is a `session.execute()` call
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the result is made available to the client).
   * @param executionProfile the execution profile of this request.
   * @param node the node that returned the successful response.
   * @param sessionRequestLogPrefix the dedicated log prefix for this request
   */
  default void onSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String sessionRequestLogPrefix) {
    // If client doesn't override onSuccess with sessionRequestLogPrefix delegate call to the old
    // method
    onSuccess(request, latencyNanos, executionProfile, node);
  }

  /**
   * @deprecated This method only exists for backward compatibility. Override {@link
   *     #onError(Request, Throwable, long, DriverExecutionProfile, Node, String)} instead.
   */
  @Deprecated
  default void onError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @Nullable Node node) {}

  /**
   * Invoked each time a session request fails. A session request is a `session.execute()` call
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the error is propagated to the client).
   * @param executionProfile the execution profile of this request.
   * @param node the node that returned the error response, or {@code null} if the error occurred
   * @param sessionRequestLogPrefix the dedicated log prefix for this request
   */
  default void onError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @Nullable Node node,
      @NonNull String sessionRequestLogPrefix) {
    // If client doesn't override onError with sessionRequestLogPrefix delegate call to the old
    // method
    onError(request, error, latencyNanos, executionProfile, node);
  }

  /**
   * @deprecated This method only exists for backward compatibility. Override {@link
   *     #onNodeError(Request, Throwable, long, DriverExecutionProfile, Node, String)} instead.
   */
  @Deprecated
  default void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node) {}

  /**
   * Invoked each time a node request fails. A node request is a CQL request sent to a particular
   * node. There can be one or more node requests for a single session request, due to retries or
   * speculative executions.
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the error is propagated to the client).
   * @param executionProfile the execution profile of this request.
   * @param node the node that returned the error response.
   * @param nodeRequestLogPrefix the dedicated log prefix for this request
   */
  default void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String nodeRequestLogPrefix) {
    // If client doesn't override onNodeError with nodeRequestLogPrefix delegate call to the old
    // method
    onNodeError(request, error, latencyNanos, executionProfile, node);
  }

  /**
   * @deprecated This method only exists for backward compatibility. Override {@link
   *     #onNodeSuccess(Request, long, DriverExecutionProfile, Node, String)} instead.
   */
  @Deprecated
  default void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node) {}

  /**
   * Invoked each time a node request succeeds. A node request is a CQL request sent to a particular
   * node. There can be one or more node requests for a single session request, due to retries or
   * speculative executions.
   *
   * @param latencyNanos the overall execution time (from the {@link Session#execute(Request,
   *     GenericType) session.execute} call until the result is made available to the client).
   * @param executionProfile the execution profile of this request.
   * @param node the node that returned the successful response.
   * @param nodeRequestLogPrefix the dedicated log prefix for this request
   */
  default void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String nodeRequestLogPrefix) {
    // If client doesn't override onNodeSuccess with nodeRequestLogPrefix delegate call to the old
    // method
    onNodeSuccess(request, latencyNanos, executionProfile, node);
  }

  /**
   * Invoked when the session is ready to process user requests.
   *
   * <p><b>WARNING: if you use {@code session.execute()} in your tracker implementation, keep in
   * mind that those requests will in turn recurse back into {@code onSuccess} / {@code onError}
   * methods.</b> Make sure you don't trigger an infinite loop; one way to do that is to use a
   * custom execution profile for internal requests.
   *
   * <p>This corresponds to the moment when {@link SessionBuilder#build()} returns, or the future
   * returned by {@link SessionBuilder#buildAsync()} completes. If the session initialization fails,
   * this method will not get called.
   *
   * <p>Listener methods are invoked from different threads; if you store the session in a field,
   * make it at least volatile to guarantee proper publication.
   *
   * <p>This method is guaranteed to be the first one invoked on this object.
   *
   * <p>The default implementation is empty.
   */
  default void onSessionReady(@NonNull Session session) {}
}
