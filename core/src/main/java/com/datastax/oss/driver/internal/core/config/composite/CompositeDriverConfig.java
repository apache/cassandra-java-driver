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
package com.datastax.oss.driver.internal.core.config.composite;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class CompositeDriverConfig implements DriverConfig {

  private final DriverConfig primaryConfig;
  private final DriverConfig fallbackConfig;
  private final Map<String, CompositeDriverExecutionProfile> profiles = new ConcurrentHashMap<>();

  public CompositeDriverConfig(
      @NonNull DriverConfig primaryConfig, @NonNull DriverConfig fallbackConfig) {
    this.primaryConfig = Objects.requireNonNull(primaryConfig);
    this.fallbackConfig = Objects.requireNonNull(fallbackConfig);
  }

  @NonNull
  @Override
  public DriverExecutionProfile getProfile(@NonNull String profileName) {
    return profiles.compute(
        profileName,
        (k, v) ->
            (v == null)
                ? new CompositeDriverExecutionProfile(primaryConfig, fallbackConfig, profileName)
                : v.refresh());
  }

  @NonNull
  @Override
  public Map<String, ? extends DriverExecutionProfile> getProfiles() {
    // The map is updated lazily, if we want all the profiles we need to fetch them explicitly
    for (String name :
        Sets.union(primaryConfig.getProfiles().keySet(), fallbackConfig.getProfiles().keySet())) {
      getProfile(name);
    }
    return Collections.unmodifiableMap(profiles);
  }
}
