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
package com.datastax.oss.driver.internal.core.config.typesafe;

import static com.typesafe.config.ConfigValueType.OBJECT;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import java.util.Collection;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class TypesafeDriverConfig implements DriverConfig {

  private static final Logger LOG = LoggerFactory.getLogger(TypesafeDriverConfig.class);

  private final Collection<DriverOption> options;
  private final Map<String, TypesafeDriverConfigProfile.Base> profiles;
  // Only used to detect if reload saw any change
  private volatile Config lastLoadedConfig;

  public TypesafeDriverConfig(Config config, DriverOption[]... optionArrays) {
    this.lastLoadedConfig = config;
    this.options = merge(optionArrays);

    Map<String, Config> profileConfigs = extractProfiles(config);

    ImmutableMap.Builder<String, TypesafeDriverConfigProfile.Base> builder = ImmutableMap.builder();
    for (Map.Entry<String, Config> entry : profileConfigs.entrySet()) {
      builder.put(
          entry.getKey(), new TypesafeDriverConfigProfile.Base(entry.getKey(), entry.getValue()));
    }
    this.profiles = builder.build();
  }

  /** @return whether the configuration changed */
  public boolean reload(Config config) {
    if (config.equals(lastLoadedConfig)) {
      return false;
    } else {
      lastLoadedConfig = config;
      try {
        Map<String, Config> profileConfigs = extractProfiles(config);
        for (Map.Entry<String, Config> entry : profileConfigs.entrySet()) {
          String profileName = entry.getKey();
          TypesafeDriverConfigProfile.Base profile = this.profiles.get(profileName);
          if (profile == null) {
            LOG.warn(
                "Unknown profile '{}' while reloading configuration. "
                    + "Adding profiles at runtime is not supported.",
                profileName);
          } else {
            profile.refresh(entry.getValue());
          }
        }
        return true;
      } catch (Throwable t) {
        Loggers.warnWithException(LOG, "Error reloading configuration, keeping previous one", t);
        return false;
      }
    }
  }

  /*
   * Processes the raw configuration to extract profiles. For example:
   *     {
   *         foo = 1, bar = 2
   *         profiles {
   *             custom1 { bar = 3 }
   *         }
   *     }
   * Would produce:
   *     "default" => { foo = 1, bar = 2 }
   *     "custom1" => { foo = 1, bar = 3 }
   */
  private Map<String, Config> extractProfiles(Config sourceConfig) {
    ImmutableMap.Builder<String, Config> result = ImmutableMap.builder();

    Config defaultProfileConfig = sourceConfig.withoutPath("profiles");
    validateRequired(defaultProfileConfig, options);
    result.put(DriverConfigProfile.DEFAULT_NAME, defaultProfileConfig);

    // The rest of the method is a bit confusing because we navigate between Typesafe config's two
    // APIs, see https://github.com/typesafehub/config#understanding-config-and-configobject
    // In an attempt to clarify:
    //    xxxObject = `ConfigObject` API (config as a hierarchical structure)
    //    xxxConfig = `Config` API (config as a flat set of options with hierarchical paths)
    ConfigObject rootObject = sourceConfig.root();
    if (rootObject.containsKey("profiles") && rootObject.get("profiles").valueType() == OBJECT) {
      ConfigObject profilesObject = (ConfigObject) rootObject.get("profiles");
      for (String profileName : profilesObject.keySet()) {
        if (profileName.equals(DriverConfigProfile.DEFAULT_NAME)) {
          throw new IllegalArgumentException(
              String.format(
                  "Can't have %s as a profile name because it's used internally. Pick another name.",
                  profileName));
        }
        ConfigValue profileObject = profilesObject.get(profileName);
        if (profileObject.valueType() == OBJECT) {
          Config profileConfig = ((ConfigObject) profileObject).toConfig();
          result.put(profileName, profileConfig.withFallback(defaultProfileConfig));
        }
      }
    }
    return result.build();
  }

  @Override
  public DriverConfigProfile getNamedProfile(String profileName) {
    Preconditions.checkArgument(
        profiles.containsKey(profileName),
        "Unknown profile '%s'. Check your configuration.",
        profileName);
    return profiles.get(profileName);
  }

  @Override
  public Map<String, DriverConfigProfile> getNamedProfiles() {
    return ImmutableMap.copyOf(profiles);
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
