/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

/**
 * The configuration of the driver.
 *
 * <p>Is is composed of options, that are organized into profiles. There is a default profile that
 * is always present, and additional, named profiles, that can override part of the options.
 * Profiles can be used to categorize queries that use the same parameters (for example, an
 * "analytics" profile vs. a "transactional" profile).
 */
public interface DriverConfig {
  DriverConfigProfile defaultProfile();

  DriverConfigProfile getProfile(String profileName);
}
