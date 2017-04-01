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
package com.datastax.oss.driver.internal.core.config.typesafe;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.typesafe.config.ConfigValueType.OBJECT;

public class TypeSafeDriverConfig implements DriverConfig {

  private final TypesafeDriverConfigProfile defaultProfile;
  private final ConcurrentMap<String, TypesafeDriverConfigProfile> profiles;

  public TypeSafeDriverConfig(Config config, DriverOption[]... optionArrays) {
    Collection<DriverOption> options = merge(optionArrays);

    // Process the raw configuration to extract profiles. For example:
    //     {
    //         foo = 1, bar = 2
    //         profiles {
    //             custom1 { bar = 3 }
    //         }
    //     }
    // Would produce a map with the following entries:
    //     "default" => { foo = 1, bar = 2 }
    //     "custom1" => { foo = 1, bar = 3 }

    Config defaultProfileConfig = config.withoutPath("profiles");
    validateRequired(defaultProfileConfig, options);
    this.defaultProfile = new TypesafeDriverConfigProfile(defaultProfileConfig);

    this.profiles = new ConcurrentHashMap<>();
    ConfigObject rootObject = config.root();
    if (rootObject.containsKey("profiles") && rootObject.get("profiles").valueType() == OBJECT) {
      ConfigObject profileConfigs = (ConfigObject) rootObject.get("profiles");
      for (String profileName : profileConfigs.keySet()) {
        ConfigValue profileValue = profileConfigs.get(profileName);
        if (profileValue.valueType() == OBJECT) {
          Config profileConfig = ((ConfigObject) profileValue).toConfig();
          this.profiles.put(
              profileName,
              new TypesafeDriverConfigProfile(profileConfig.withFallback(defaultProfileConfig)));
        }
      }
    }
  }

  @Override
  public DriverConfigProfile defaultProfile() {
    return defaultProfile;
  }

  @Override
  public DriverConfigProfile getProfile(String profileName) {
    Preconditions.checkArgument(
        profiles.containsKey(profileName),
        "Unknown profile '%s'. Check your configuration.",
        profileName);
    return profiles.get(profileName);
  }

  private static void validateRequired(Config config, Collection<DriverOption> options) {
    for (DriverOption option : options) {
      Preconditions.checkArgument(
          !option.required() || config.hasPath(option.getPath()),
          "Missing option %s. Check your configuration file.",
          option.getPath());
    }
  }

  private Collection<DriverOption> merge(DriverOption[][] optionArrays) {
    Preconditions.checkArgument(optionArrays.length > 0, "Must provide some options");
    ImmutableList.Builder<DriverOption> optionsBuilder = ImmutableList.builder();
    for (DriverOption[] optionArray : optionArrays) {
      for (DriverOption driverOption : optionArray) {
        optionsBuilder.add(driverOption);
      }
    }
    return optionsBuilder.build();
  }
}
