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
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigOrigin;
import com.typesafe.config.ConfigOriginFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class TypesafeDriverConfig implements DriverConfig {

  private static final Logger LOG = LoggerFactory.getLogger(TypesafeDriverConfig.class);
  private static final ConfigOrigin DEFAULT_OVERRIDES_ORIGIN =
      ConfigOriginFactory.newSimple("default was overridden programmatically");

  private final ImmutableMap<String, TypesafeDriverExecutionProfile.Base> profiles;
  // Only used to detect if reload saw any change
  private volatile Config lastLoadedConfig;

  private final Map<DriverOption, Object> defaultOverrides = new ConcurrentHashMap<>();

  public TypesafeDriverConfig(Config config) {
    this.lastLoadedConfig = config;
    Map<String, Config> profileConfigs = extractProfiles(config);

    ImmutableMap.Builder<String, TypesafeDriverExecutionProfile.Base> builder =
        ImmutableMap.builder();
    for (Map.Entry<String, Config> entry : profileConfigs.entrySet()) {
      builder.put(
          entry.getKey(),
          new TypesafeDriverExecutionProfile.Base(entry.getKey(), entry.getValue()));
    }
    this.profiles = builder.build();
  }

  /** @return whether the configuration changed */
  public boolean reload(Config config) {
    config = applyDefaultOverrides(config);
    if (config.equals(lastLoadedConfig)) {
      return false;
    } else {
      lastLoadedConfig = config;
      try {
        Map<String, Config> profileConfigs = extractProfiles(config);
        for (Map.Entry<String, Config> entry : profileConfigs.entrySet()) {
          String profileName = entry.getKey();
          TypesafeDriverExecutionProfile.Base profile = this.profiles.get(profileName);
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
    result.put(DriverExecutionProfile.DEFAULT_NAME, defaultProfileConfig);

    // The rest of the method is a bit confusing because we navigate between Typesafe config's two
    // APIs, see https://github.com/typesafehub/config#understanding-config-and-configobject
    // In an attempt to clarify:
    //    xxxObject = `ConfigObject` API (config as a hierarchical structure)
    //    xxxConfig = `Config` API (config as a flat set of options with hierarchical paths)
    ConfigObject rootObject = sourceConfig.root();
    if (rootObject.containsKey("profiles") && rootObject.get("profiles").valueType() == OBJECT) {
      ConfigObject profilesObject = (ConfigObject) rootObject.get("profiles");
      for (String profileName : profilesObject.keySet()) {
        if (profileName.equals(DriverExecutionProfile.DEFAULT_NAME)) {
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

  @NonNull
  @Override
  public DriverExecutionProfile getProfile(@NonNull String profileName) {
    Preconditions.checkArgument(
        profiles.containsKey(profileName),
        "Unknown profile '%s'. Check your configuration.",
        profileName);
    return profiles.get(profileName);
  }

  @NonNull
  @Override
  public Map<String, ? extends DriverExecutionProfile> getProfiles() {
    return profiles;
  }

  /**
   * Replace the given options, <em>only if the original values came from {@code
   * reference.conf}</em>: if the option was set explicitly in {@code application.conf}, then the
   * override is ignored.
   *
   * <p>The overrides are also taken into account in profiles, and survive reloads. If this method
   * is invoked multiple times, the last value for each option will be used. Note that it is
   * currently not possible to use {@code null} as a value.
   */
  public void overrideDefaults(@NonNull Map<DriverOption, Object> overrides) {
    defaultOverrides.putAll(overrides);
    reload(lastLoadedConfig);
  }

  private Config applyDefaultOverrides(Config source) {
    Config result = source;
    for (Map.Entry<DriverOption, Object> entry : defaultOverrides.entrySet()) {
      String path = entry.getKey().getPath();
      Object value = entry.getValue();
      if (isDefault(source, path)) {
        LOG.debug("Replacing default value for {} by {}", path, value);
        result =
            result.withValue(
                path, ConfigValueFactory.fromAnyRef(value).withOrigin(DEFAULT_OVERRIDES_ORIGIN));
      } else {
        LOG.debug(
            "Ignoring default override for {} because the user has overridden the value", path);
      }
    }
    return result;
  }

  // Whether the value in the given path comes from the reference.conf in the driver JAR.
  private static boolean isDefault(Config config, String path) {
    if (!config.hasPath(path)) {
      return false;
    }
    ConfigOrigin origin = config.getValue(path).origin();
    if (origin.equals(DEFAULT_OVERRIDES_ORIGIN)) {
      // Same default was overridden twice, should use the last value
      return true;
    }
    URL url = origin.url();
    return url != null && url.toString().endsWith("reference.conf");
  }
}
