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
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class DefaultProgrammaticDriverConfigLoaderBuilder
    implements ProgrammaticDriverConfigLoaderBuilder {

  public static final Supplier<Config> DEFAULT_FALLBACK_SUPPLIER =
      () ->
          ConfigFactory.defaultApplication()
              .withFallback(ConfigFactory.defaultReference(DriverConfigLoader.DRIVER_CLASS_LOADER));

  private final NullAllowingImmutableMap.Builder<String, Object> values =
      NullAllowingImmutableMap.builder();
  private final Supplier<Config> fallbackSupplier;
  private final String rootPath;

  private String currentProfileName = DriverExecutionProfile.DEFAULT_NAME;

  /**
   * @param fallbackSupplier the supplier that will provide fallback configuration for options that
   *     haven't been specified programmatically.
   * @param rootPath the root path used in non-programmatic sources (fallback reference.conf and
   *     system properties).
   */
  public DefaultProgrammaticDriverConfigLoaderBuilder(
      Supplier<Config> fallbackSupplier, String rootPath) {
    this.fallbackSupplier = fallbackSupplier;
    this.rootPath = rootPath;
  }

  public DefaultProgrammaticDriverConfigLoaderBuilder() {
    this(DEFAULT_FALLBACK_SUPPLIER, DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
  }

  /**
   * Creates an instance of {@link DefaultProgrammaticDriverConfigLoaderBuilder} that locates
   * application configuration resources using the provided {@link ClassLoader} instead of the
   * driver's {@linkplain DriverConfigLoader#DRIVER_CLASS_LOADER default one}.
   */
  public DefaultProgrammaticDriverConfigLoaderBuilder(ClassLoader appClassLoader) {
    this(
        () ->
            ConfigFactory.defaultApplication(appClassLoader)
                .withFallback(
                    ConfigFactory.defaultReference(DriverConfigLoader.DRIVER_CLASS_LOADER)),
        DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
  }

  private ProgrammaticDriverConfigLoaderBuilder with(
      @NonNull DriverOption option, @Nullable Object value) {
    return with(option.getPath(), value);
  }

  private ProgrammaticDriverConfigLoaderBuilder with(@NonNull String path, @Nullable Object value) {
    if (!DriverExecutionProfile.DEFAULT_NAME.equals(currentProfileName)) {
      path = "profiles." + currentProfileName + "." + path;
    }
    if (!rootPath.isEmpty()) {
      path = rootPath + "." + path;
    }
    values.put(path, value);
    return this;
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder startProfile(@NonNull String profileName) {
    currentProfileName = Objects.requireNonNull(profileName);
    return this;
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder endProfile() {
    currentProfileName = DriverExecutionProfile.DEFAULT_NAME;
    return this;
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withBoolean(
      @NonNull DriverOption option, boolean value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withBooleanList(
      @NonNull DriverOption option, @NonNull List<Boolean> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withInt(@NonNull DriverOption option, int value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withIntList(
      @NonNull DriverOption option, @NonNull List<Integer> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withLong(@NonNull DriverOption option, long value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withLongList(
      @NonNull DriverOption option, @NonNull List<Long> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withDouble(
      @NonNull DriverOption option, double value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withDoubleList(
      @NonNull DriverOption option, @NonNull List<Double> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withString(
      @NonNull DriverOption option, @NonNull String value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withStringList(
      @NonNull DriverOption option, @NonNull List<String> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withStringMap(
      @NonNull DriverOption option, @NonNull Map<String, String> value) {
    for (String key : value.keySet()) {
      this.with(option.getPath() + "." + key, value.get(key));
    }
    return this;
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withBytes(@NonNull DriverOption option, long value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withBytesList(
      @NonNull DriverOption option, @NonNull List<Long> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withDuration(
      @NonNull DriverOption option, @NonNull Duration value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withDurationList(
      @NonNull DriverOption option, @NonNull List<Duration> value) {
    return with(option, value);
  }

  @NonNull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder without(@NonNull DriverOption option) {
    return with(option, null);
  }

  @NonNull
  @Override
  public DriverConfigLoader build() {
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config programmaticConfig = buildConfig();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(programmaticConfig)
                  .withFallback(fallbackSupplier.get())
                  .resolve();
          return rootPath.isEmpty() ? config : config.getConfig(rootPath);
        });
  }

  private Config buildConfig() {
    Config config = ConfigFactory.empty();
    for (Map.Entry<String, Object> entry : values.build().entrySet()) {
      config = config.withValue(entry.getKey(), ConfigValueFactory.fromAnyRef(entry.getValue()));
    }
    return config;
  }
}
