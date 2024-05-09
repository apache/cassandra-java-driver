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
package com.datastax.oss.driver.api.core.config;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Describes an option in the driver's configuration.
 *
 * <p>This is just a thin wrapper around the option's path, to make it easier to find where it is
 * referenced in the code. We recommend using enums for implementations.
 */
public interface DriverOption {

  /**
   * The option's path. Paths are hierarchical and each segment is separated by a dot, e.g. {@code
   * metadata.schema.enabled}.
   */
  @NonNull
  String getPath();
}
