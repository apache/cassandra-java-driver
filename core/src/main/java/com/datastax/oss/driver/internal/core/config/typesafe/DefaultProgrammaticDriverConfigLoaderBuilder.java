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
package com.datastax.oss.driver.internal.core.config.typesafe;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class DefaultProgrammaticDriverConfigLoaderBuilder
    implements ProgrammaticDriverConfigLoaderBuilder {

  public static final Supplier<Config> DEFAULT_FALLBACK_SUPPLIER =
      () ->
          ConfigFactory.defaultApplication()
              // Do not remove root path here, it must be done after merging configs
              .withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader()));

  private final Map<String, Object> values = new HashMap<>();

  private final Supplier<Config> fallbackSupplier;
  private final String rootPath;

  private String currentProfileName = DriverExecutionProfile.DEFAULT_NAME;

  /**
   * Creates an instance of {@link DefaultProgrammaticDriverConfigLoaderBuilder} with default
   * settings.
   *
   * <p>Fallback configuration for options that haven't been specified programmatically will be
   * obtained from standard classpath resources. Application-specific classpath resources will be
   * located using the {@linkplain Thread#getContextClassLoader() the current thread's context class
   * loader}. This might not be suitable for OSGi deployments, which should use {@link
   * #DefaultProgrammaticDriverConfigLoaderBuilder(ClassLoader)} instead.
   */
  public DefaultProgrammaticDriverConfigLoaderBuilder() {
    this(DEFAULT_FALLBACK_SUPPLIER, DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
  }

  /**
   * Creates an instance of {@link DefaultProgrammaticDriverConfigLoaderBuilder} with default
   * settings but a custom class loader.
   *
   * <p>Fallback configuration for options that haven't been specified programmatically will be
   * obtained from standard classpath resources. Application-specific classpath resources will be
   * located using the provided {@link ClassLoader} instead of {@linkplain
   * Thread#getContextClassLoader() the current thread's context class loader}.
   */
  public DefaultProgrammaticDriverConfigLoaderBuilder(@Nonnull ClassLoader appClassLoader) {
    this(
        () ->
            ConfigFactory.defaultApplication(appClassLoader)
                .withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader())),
        DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
  }

  /**
   * Creates an instance of {@link DefaultProgrammaticDriverConfigLoaderBuilder} using a custom
   * fallback config supplier.
   *
   * @param fallbackSupplier the supplier that will provide fallback configuration for options that
   *     haven't been specified programmatically.
   * @param rootPath the root path used in non-programmatic sources (fallback reference.conf and
   *     system properties). In most cases it should be {@link
   *     DefaultDriverConfigLoader#DEFAULT_ROOT_PATH}. Cannot be null but can be empty.
   */
  public DefaultProgrammaticDriverConfigLoaderBuilder(
      @Nonnull Supplier<Config> fallbackSupplier, @Nonnull String rootPath) {
    this.fallbackSupplier = fallbackSupplier;
    this.rootPath = rootPath;
  }

  private ProgrammaticDriverConfigLoaderBuilder with(
      @Nonnull DriverOption option, @Nullable Object value) {
    return with(option.getPath(), value);
  }

  private ProgrammaticDriverConfigLoaderBuilder with(@Nonnull String path, @Nullable Object value) {
    if (!DriverExecutionProfile.DEFAULT_NAME.equals(currentProfileName)) {
      path = "profiles." + currentProfileName + "." + path;
    }
    if (!rootPath.isEmpty()) {
      path = rootPath + "." + path;
    }
    values.put(path, value);
    return this;
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder startProfile(@Nonnull String profileName) {
    currentProfileName = Objects.requireNonNull(profileName);
    return this;
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder endProfile() {
    currentProfileName = DriverExecutionProfile.DEFAULT_NAME;
    return this;
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withBoolean(
      @Nonnull DriverOption option, boolean value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withBooleanList(
      @Nonnull DriverOption option, @Nonnull List<Boolean> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withInt(@Nonnull DriverOption option, int value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withIntList(
      @Nonnull DriverOption option, @Nonnull List<Integer> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withLong(@Nonnull DriverOption option, long value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withLongList(
      @Nonnull DriverOption option, @Nonnull List<Long> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withDouble(
      @Nonnull DriverOption option, double value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withDoubleList(
      @Nonnull DriverOption option, @Nonnull List<Double> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withString(
      @Nonnull DriverOption option, @Nonnull String value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withStringList(
      @Nonnull DriverOption option, @Nonnull List<String> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withStringMap(
      @Nonnull DriverOption option, @Nonnull Map<String, String> value) {
    for (String key : value.keySet()) {
      this.with(option.getPath() + "." + key, value.get(key));
    }
    return this;
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withBytes(@Nonnull DriverOption option, long value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withBytesList(
      @Nonnull DriverOption option, @Nonnull List<Long> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withDuration(
      @Nonnull DriverOption option, @Nonnull Duration value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder withDurationList(
      @Nonnull DriverOption option, @Nonnull List<Duration> value) {
    return with(option, value);
  }

  @Nonnull
  @Override
  public ProgrammaticDriverConfigLoaderBuilder without(@Nonnull DriverOption option) {
    return with(option, null);
  }

  @Nonnull
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
          // Only remove rootPath after the merge between system properties
          // and fallback configuration, since both are supposed to
          // contain the same rootPath prefix.
          return rootPath.isEmpty() ? config : config.getConfig(rootPath);
        });
  }

  private Config buildConfig() {
    Config config = ConfigFactory.empty();
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      config = config.withValue(entry.getKey(), ConfigValueFactory.fromAnyRef(entry.getValue()));
    }
    return config;
  }
}
