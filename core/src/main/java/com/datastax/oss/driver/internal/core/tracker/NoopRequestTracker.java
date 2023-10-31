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
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import javax.annotation.Nonnull;
import net.jcip.annotations.ThreadSafe;

/**
 * Default request tracker implementation with empty methods. This implementation is used when no
 * trackers were registered, neither programmatically nor through the configuration.
 */
@ThreadSafe
public class NoopRequestTracker implements RequestTracker {

  public NoopRequestTracker(@SuppressWarnings("unused") DriverContext context) {
    // nothing to do
  }

  @Override
  public void onSuccess(
      @Nonnull Request request,
      long latencyNanos,
      @Nonnull DriverExecutionProfile executionProfile,
      @Nonnull Node node,
      @Nonnull String requestPrefix) {
    // nothing to do
  }

  @Override
  public void onError(
      @Nonnull Request request,
      @Nonnull Throwable error,
      long latencyNanos,
      @Nonnull DriverExecutionProfile executionProfile,
      Node node,
      @Nonnull String requestPrefix) {
    // nothing to do
  }

  @Override
  public void onNodeError(
      @Nonnull Request request,
      @Nonnull Throwable error,
      long latencyNanos,
      @Nonnull DriverExecutionProfile executionProfile,
      @Nonnull Node node,
      @Nonnull String requestPrefix) {
    // nothing to do
  }

  @Override
  public void onNodeSuccess(
      @Nonnull Request request,
      long latencyNanos,
      @Nonnull DriverExecutionProfile executionProfile,
      @Nonnull Node node,
      @Nonnull String requestPrefix) {
    // nothing to do
  }

  @Override
  public void close() throws Exception {
    // nothing to do
  }
}
