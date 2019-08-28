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

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;

/**
 * @deprecated this feature is now available in the public API. Use {@link
 *     DriverConfigLoader#programmaticBuilder()} instead.
 */
@NotThreadSafe
@Deprecated
public class DefaultDriverConfigLoaderBuilder
    implements com.datastax.oss.driver.internal.core.config.DriverOptionConfigBuilder<
        DefaultDriverConfigLoaderBuilder> {

  private NullAllowingImmutableMap.Builder<String, Object> values =
      NullAllowingImmutableMap.builder();

  /**
   * @return a new {@link ProfileBuilder} to provide programmatic configuration at a profile level.
   * @see #withProfile(String, Profile)
   */
  @NonNull
  public static ProfileBuilder profileBuilder() {
    return new ProfileBuilder();
  }

  /** Adds configuration for a profile constructed using {@link #profileBuilder()} by name. */
  @NonNull
  public DefaultDriverConfigLoaderBuilder withProfile(
      @NonNull String profileName, @NonNull Profile profile) {
    String prefix = "profiles." + profileName + ".";
    for (Map.Entry<String, Object> entry : profile.values.entrySet()) {
      this.with(prefix + entry.getKey(), entry.getValue());
    }
    return this;
  }

  /**
   * @return constructed {@link DriverConfigLoader} using the configuration passed into this
   *     builder.
   */
  @NonNull
  public DriverConfigLoader build() {
    // fallback on the default config supplier (config file)
    return new DefaultDriverConfigLoader(
        () -> buildConfig().withFallback(DefaultDriverConfigLoader.DEFAULT_CONFIG_SUPPLIER.get()));
  }

  /** @return A {@link Config} containing only the options provided */
  protected Config buildConfig() {
    Config config = ConfigFactory.empty();
    for (Map.Entry<String, Object> entry : values.build().entrySet()) {
      config = config.withValue(entry.getKey(), ConfigValueFactory.fromAnyRef(entry.getValue()));
    }
    return config;
  }

  @NonNull
  @Override
  public DefaultDriverConfigLoaderBuilder with(@NonNull String path, @Nullable Object value) {
    values.put(path, value);
    return this;
  }

  /** A builder for specifying options at a profile level using {@code withXXX} methods. */
  @Deprecated
  public static final class ProfileBuilder
      implements com.datastax.oss.driver.internal.core.config.DriverOptionConfigBuilder<
          ProfileBuilder> {

    final NullAllowingImmutableMap.Builder<String, Object> values =
        NullAllowingImmutableMap.builder();

    private ProfileBuilder() {}

    @NonNull
    @Override
    public ProfileBuilder with(@NonNull String path, @Nullable Object value) {
      values.put(path, value);
      return this;
    }

    @NonNull
    public Profile build() {
      return new Profile(values.build());
    }
  }

  /**
   * A single-purpose holder of profile options as a map to be consumed by {@link
   * DefaultDriverConfigLoaderBuilder}.
   */
  public static final class Profile {
    final Map<String, Object> values;

    private Profile(Map<String, Object> values) {
      this.values = values;
    }
  }
}
