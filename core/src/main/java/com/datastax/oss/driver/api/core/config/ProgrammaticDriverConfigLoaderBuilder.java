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
 * A builder that allows the creation of a config loader where options are overridden
 * programmatically.
 *
 * @see DriverConfigLoader#programmaticBuilder()
 */
public interface ProgrammaticDriverConfigLoaderBuilder
    extends OngoingConfigOptions<ProgrammaticDriverConfigLoaderBuilder> {

  /**
   * Starts the definition of a new profile.
   *
   * <p>All options set after this call, and before the next call to this method or {@link
   * #endProfile()}, will apply to the given profile.
   */
  @NonNull
  ProgrammaticDriverConfigLoaderBuilder startProfile(@NonNull String profileName);

  /**
   * Ends the definition of a profile.
   *
   * <p>All options set after this call, and before the next call to {@link #startProfile(String)},
   * will apply to the default profile.
   */
  @NonNull
  ProgrammaticDriverConfigLoaderBuilder endProfile();

  @NonNull
  DriverConfigLoader build();
}
