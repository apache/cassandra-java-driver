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

/**
 * Manages the initialization, and optionally the periodic reloading, of the driver configuration.
 */
public interface DriverConfigLoader extends AutoCloseable {

  /** Loads the first configuration that will be used to initialize the driver. */
  DriverConfig getInitialConfig();

  /**
   * Called when the driver initializes. For loaders that periodically check for configuration
   * updates, this is a good time to grab an internal executor and schedule a recurring task.
   */
  void onDriverInit(DriverContext context);

  /**
   * Called when the cluster closes. This is a good time to release any external resource, for
   * example cancel a scheduled reloading task.
   */
  void close();
}
