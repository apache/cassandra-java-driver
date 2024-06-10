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
package com.datastax.oss.driver.internal.core.config.map;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** @see MapBasedDriverConfigLoader */
public class MapBasedDriverConfig implements DriverConfig {

  private final Map<String, Map<DriverOption, Object>> optionsMap;
  private final Map<String, MapBasedDriverExecutionProfile> profiles = new ConcurrentHashMap<>();

  public MapBasedDriverConfig(Map<String, Map<DriverOption, Object>> optionsMap) {
    this.optionsMap = optionsMap;
    if (!optionsMap.containsKey(DriverExecutionProfile.DEFAULT_NAME)) {
      throw new IllegalArgumentException(
          "The options map must contain a profile named " + DriverExecutionProfile.DEFAULT_NAME);
    }
    createMissingProfiles();
  }

  @NonNull
  @Override
  public DriverExecutionProfile getProfile(@NonNull String profileName) {
    return profiles.computeIfAbsent(profileName, this::newProfile);
  }

  @NonNull
  @Override
  public Map<String, ? extends DriverExecutionProfile> getProfiles() {
    // Refresh in case profiles were added to the backing map
    createMissingProfiles();
    return Collections.unmodifiableMap(profiles);
  }

  private void createMissingProfiles() {
    for (Map.Entry<String, Map<DriverOption, Object>> entry : optionsMap.entrySet()) {
      String profileName = entry.getKey();
      if (!profiles.containsKey(profileName)) {
        profiles.put(profileName, newProfile(profileName));
      }
    }
  }

  private MapBasedDriverExecutionProfile newProfile(String profileName) {
    return new MapBasedDriverExecutionProfile(optionsMap, profileName);
  }
}
