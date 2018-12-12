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
package com.datastax.oss.driver.api.core.config;

import com.datastax.oss.driver.api.core.context.DriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletionStage;

/**
 * Manages the initialization, and optionally the periodic reloading, of the driver configuration.
 */
public interface DriverConfigLoader extends AutoCloseable {

  /**
   * Loads the first configuration that will be used to initialize the driver.
   *
   * <p>If this loader {@linkplain #supportsReloading() supports reloading}, this object should be
   * mutable and reflect later changes when the configuration gets reloaded.
   */
  @NonNull
  DriverConfig getInitialConfig();

  /**
   * Called when the driver initializes. For loaders that periodically check for configuration
   * updates, this is a good time to grab an internal executor and schedule a recurring task.
   */
  void onDriverInit(@NonNull DriverContext context);

  /**
   * Triggers an immediate reload attempt.
   *
   * @return a stage that completes once the attempt is finished, with a boolean indicating whether
   *     the configuration changed as a result of this reload. If so, it's also guaranteed that
   *     internal driver components have been notified by that time; note however that some react to
   *     the notification asynchronously, so they may not have completely applied all resulting
   *     changes yet. If this loader does not support programmatic reloading &mdash; which you can
   *     check by calling {@link #supportsReloading()} before this method &mdash; the returned
   *     object will fail immediately with an {@link UnsupportedOperationException}.
   */
  @NonNull
  CompletionStage<Boolean> reload();

  /**
   * Whether this implementation supports programmatic reloading with the {@link #reload()} method.
   */
  boolean supportsReloading();

  /**
   * Called when the cluster closes. This is a good time to release any external resource, for
   * example cancel a scheduled reloading task.
   */
  @Override
  void close();
}
