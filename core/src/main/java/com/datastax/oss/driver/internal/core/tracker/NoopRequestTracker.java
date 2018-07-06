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
package com.datastax.oss.driver.internal.core.tracker;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;

/**
 * A no-op request tracker.
 *
 * <p>To activate this tracker, modify the {@code advanced.request-tracker} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.request-tracker {
 *     class = NoopRequestTracker
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 *
 * <p>Note that if a tracker is specified programmatically with {@link
 * SessionBuilder#withRequestTracker(RequestTracker)}, the configuration is ignored.
 */
@ThreadSafe
public class NoopRequestTracker implements RequestTracker {

  public NoopRequestTracker(@SuppressWarnings("unused") DriverContext context) {
    // nothing to do
  }

  @Override
  public void onSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node) {
    // nothing to do
  }

  @Override
  public void onError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      Node node) {
    // nothing to do
  }

  @Override
  public void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node) {
    // nothing to do
  }

  @Override
  public void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node) {
    // nothing to do
  }

  @Override
  public void close() throws Exception {
    // nothing to do
  }
}
