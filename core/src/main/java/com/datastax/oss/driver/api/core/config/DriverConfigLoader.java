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
package com.datastax.oss.driver.api.core.config;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.internal.core.config.composite.CompositeDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.map.MapBasedDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.util.concurrent.CompletionStage;

/**
 * Manages the initialization, and optionally the periodic reloading, of the driver configuration.
 *
 * @see SessionBuilder#withConfigLoader(DriverConfigLoader)
 */
public interface DriverConfigLoader extends AutoCloseable {

  /**
   * Builds an instance using the driver's default implementation (based on Typesafe config) except
   * that application-specific classpath resources will be located using the provided {@link
   * ClassLoader} instead of {@linkplain Thread#getContextClassLoader() the current thread's context
   * class loader}.
   *
   * <p>The returned loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   */
  @NonNull
  static DriverConfigLoader fromDefaults(@NonNull ClassLoader appClassLoader) {
    return new DefaultDriverConfigLoader(appClassLoader);
  }

  /**
   * Builds an instance using the driver's default implementation (based on Typesafe config), except
   * that application-specific options are loaded from a classpath resource with a custom name.
   *
   * <p>The class loader used to locate application-specific classpath resources is {@linkplain
   * Thread#getContextClassLoader() the current thread's context class loader}. This might not be
   * suitable for OSGi deployments, which should use {@link #fromClasspath(String, ClassLoader)}
   * instead.
   *
   * <p>More precisely, configuration properties are loaded and merged from the following
   * (first-listed are higher priority):
   *
   * <ul>
   *   <li>system properties
   *   <li>{@code <resourceBaseName>.conf} (all resources on classpath with this name)
   *   <li>{@code <resourceBaseName>.json} (all resources on classpath with this name)
   *   <li>{@code <resourceBaseName>.properties} (all resources on classpath with this name)
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core driver JAR, that defines
   *       default options for all mandatory options.
   * </ul>
   *
   * The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>The returned loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   */
  @NonNull
  static DriverConfigLoader fromClasspath(@NonNull String resourceBaseName) {
    return fromClasspath(resourceBaseName, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Just like {@link #fromClasspath(java.lang.String)} except that application-specific classpath
   * resources will be located using the provided {@link ClassLoader} instead of {@linkplain
   * Thread#getContextClassLoader() the current thread's context class loader}.
   */
  @NonNull
  static DriverConfigLoader fromClasspath(
      @NonNull String resourceBaseName, @NonNull ClassLoader appClassLoader) {
    return DefaultDriverConfigLoader.fromClasspath(resourceBaseName, appClassLoader);
  }

  /**
   * Builds an instance using the driver's default implementation (based on Typesafe config), except
   * that application-specific options are loaded from the given path.
   *
   * <p>More precisely, configuration properties are loaded and merged from the following
   * (first-listed are higher priority):
   *
   * <ul>
   *   <li>system properties
   *   <li>the contents of {@code file}
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core driver JAR, that defines
   *       default options for all mandatory options.
   * </ul>
   *
   * The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>The returned loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   */
  @NonNull
  static DriverConfigLoader fromPath(@NonNull Path file) {
    return fromFile(file.toFile());
  }

  /**
   * Builds an instance using the driver's default implementation (based on Typesafe config), except
   * that application-specific options are loaded from the given file.
   *
   * <p>More precisely, configuration properties are loaded and merged from the following
   * (first-listed are higher priority):
   *
   * <ul>
   *   <li>system properties
   *   <li>the contents of {@code file}
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core driver JAR, that defines
   *       default options for all mandatory options.
   * </ul>
   *
   * The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>The returned loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   */
  @NonNull
  static DriverConfigLoader fromFile(@NonNull File file) {
    return DefaultDriverConfigLoader.fromFile(file);
  }

  /**
   * Builds an instance using the driver's default implementation (based on Typesafe config), except
   * that application-specific options are loaded from the given URL.
   *
   * <p>More precisely, configuration properties are loaded and merged from the following
   * (first-listed are higher priority):
   *
   * <ul>
   *   <li>system properties
   *   <li>the contents of {@code url}
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core driver JAR, that defines
   *       default options for all mandatory options.
   * </ul>
   *
   * The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>The returned loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   */
  @NonNull
  static DriverConfigLoader fromUrl(@NonNull URL url) {
    return DefaultDriverConfigLoader.fromUrl(url);
  }

  /**
   * Builds an instance using the driver's default implementation (based on Typesafe config), except
   * that application-specific options are parsed from the given string.
   *
   * <p>The string must be in HOCON format and contain a {@code datastax-java-driver} section.
   * Options must be separated by line breaks:
   *
   * <pre>
   * DriverConfigLoader.fromString(
   *         "datastax-java-driver.basic { session-name = my-app\nrequest.timeout = 1 millisecond }")
   * </pre>
   *
   * <p>More precisely, configuration properties are loaded and merged from the following
   * (first-listed are higher priority):
   *
   * <ul>
   *   <li>system properties
   *   <li>the config in {@code contents}
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core driver JAR, that defines
   *       default options for all mandatory options.
   * </ul>
   *
   * <p>This loader does not support runtime reloading.
   */
  @NonNull
  static DriverConfigLoader fromString(@NonNull String contents) {
    return DefaultDriverConfigLoader.fromString(contents);
  }

  /**
   * Starts a builder that allows configuration options to be overridden programmatically.
   *
   * <p>Note that {@link #fromMap(OptionsMap)} provides an alternative approach for programmatic
   * configuration, that might be more convenient if you wish to completely bypass Typesafe config.
   *
   * <p>For example:
   *
   * <pre>{@code
   * DriverConfigLoader loader =
   *     DriverConfigLoader.programmaticBuilder()
   *         .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
   *         .startProfile("slow")
   *         .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
   *         .endProfile()
   *         .build();
   * }</pre>
   *
   * produces the same overrides as:
   *
   * <pre>
   * datastax-java-driver {
   *   basic.request.timeout = 5 seconds
   *   profiles {
   *     slow {
   *       basic.request.timeout = 30 seconds
   *     }
   *   }
   * }
   * </pre>
   *
   * The resulting loader still uses the driver's default implementation (based on Typesafe config),
   * except that the programmatic configuration takes precedence. More precisely, configuration
   * properties are loaded and merged from the following (first-listed are higher priority):
   *
   * <ul>
   *   <li>system properties
   *   <li>properties that were provided programmatically
   *   <li>{@code application.conf} (all resources on classpath with this name)
   *   <li>{@code application.json} (all resources on classpath with this name)
   *   <li>{@code application.properties} (all resources on classpath with this name)
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core driver JAR, that defines
   *       default options for all mandatory options.
   * </ul>
   *
   * Note that {@code application.*} is entirely optional, you may choose to only rely on the
   * driver's built-in {@code reference.conf} and programmatic overrides.
   *
   * <p>The class loader used to locate application-specific classpath resources is {@linkplain
   * Thread#getContextClassLoader() the current thread's context class loader}. This might not be
   * suitable for OSGi deployments, which should use {@link #programmaticBuilder(ClassLoader)}
   * instead.
   *
   * <p>The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>The loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   *
   * <p>Note that the returned builder is <b>not thread-safe</b>.
   *
   * @see #fromMap(OptionsMap)
   */
  @NonNull
  static ProgrammaticDriverConfigLoaderBuilder programmaticBuilder() {
    return new DefaultProgrammaticDriverConfigLoaderBuilder();
  }

  /**
   * Just like {@link #programmaticBuilder()} except that application-specific classpath resources
   * will be located using the provided {@link ClassLoader} instead of {@linkplain
   * Thread#getContextClassLoader() the current thread's context class loader}.
   */
  @NonNull
  static ProgrammaticDriverConfigLoaderBuilder programmaticBuilder(
      @NonNull ClassLoader appClassLoader) {
    return new DefaultProgrammaticDriverConfigLoaderBuilder(appClassLoader);
  }

  /**
   * Builds an instance backed by an {@link OptionsMap}, which holds all options in memory.
   *
   * <p>This is the simplest implementation. It is intended for clients who wish to completely
   * bypass Typesafe config, and instead manage the configuration programmatically. A typical
   * example is a third-party tool that already has its own configuration file, and doesn't want to
   * introduce a separate mechanism for driver options.
   *
   * <p>With this loader, the driver's built-in {@code reference.conf} file is ignored, the provided
   * {@link OptionsMap} must explicitly provide all mandatory options. Note however that {@link
   * OptionsMap#driverDefaults()} allows you to initialize an instance with the same default values
   * as {@code reference.conf}.
   *
   * <pre>
   * // This creates a configuration equivalent to the built-in reference.conf:
   * OptionsMap map = OptionsMap.driverDefaults();
   *
   * // Customize an option:
   * map.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5));
   *
   * DriverConfigLoader loader = DriverConfigLoader.fromMap(map);
   * CqlSession session = CqlSession.builder()
   *     .withConfigLoader(loader)
   *     .build();
   * </pre>
   *
   * <p>If the {@link OptionsMap} is modified at runtime, this will be reflected immediately in the
   * configuration, you don't need to call {@link #reload()}. Note however that, depending on the
   * option, the driver might not react to a configuration change immediately, or ever (this is
   * documented in {@code reference.conf}).
   *
   * @since 4.6.0
   */
  @NonNull
  static DriverConfigLoader fromMap(@NonNull OptionsMap source) {
    return new MapBasedDriverConfigLoader(source, source.asRawMap());
  }

  /**
   * Composes two existing config loaders to form a new one.
   *
   * <p>When the driver reads an option, the "primary" config will be queried first. If the option
   * is missing, then it will be looked up in the "fallback" config.
   *
   * <p>All execution profiles will be surfaced in the new config. If a profile is defined both in
   * the primary and the fallback config, its options will be merged using the same precedence rules
   * as described above.
   *
   * <p>The new config is reloadable if at least one of the input configs is. If you invoke {@link
   * DriverConfigLoader#reload()} on the new loader, it will reload whatever is reloadable, or fail
   * if nothing is. If the input loaders have periodic reloading built-in, each one will reload at
   * its own pace, and the changes will be reflected in the new config.
   */
  @NonNull
  static DriverConfigLoader compose(
      @NonNull DriverConfigLoader primaryConfigLoader,
      @NonNull DriverConfigLoader fallbackConfigLoader) {
    return new CompositeDriverConfigLoader(primaryConfigLoader, fallbackConfigLoader);
  }

  /**
   * Loads the first configuration that will be used to initialize the driver.
   *
   * <p>If this loader {@linkplain #supportsReloading() supports reloading}, this object should be
   * mutable and reflect later changes when the configuration gets reloaded.
   */
  @NonNull
  DriverConfig getInitialConfig();

  /**
   * Called when the driver initializes. For loaders that periodically check for configuration
   * updates, this is a good time to grab an internal executor and schedule a recurring task.
   */
  void onDriverInit(@NonNull DriverContext context);

  /**
   * Triggers an immediate reload attempt and returns a stage that completes once the attempt is
   * finished, with a boolean indicating whether the configuration changed as a result of this
   * reload.
   *
   * <p>If so, it's also guaranteed that internal driver components have been notified by that time;
   * note however that some react to the notification asynchronously, so they may not have
   * completely applied all resulting changes yet.
   *
   * <p>If this loader does not support programmatic reloading &mdash; which you can check by
   * calling {@link #supportsReloading()} before this method &mdash; the returned stage should fail
   * immediately with an {@link UnsupportedOperationException}. The default implementation of this
   * interface does support programmatic reloading however, and never returns a failed stage.
   */
  @NonNull
  CompletionStage<Boolean> reload();

  /**
   * Whether this implementation supports programmatic reloading with the {@link #reload()} method.
   *
   * <p>The default implementation of this interface does support programmatic reloading and always
   * returns <code>true</code>.
   */
  boolean supportsReloading();

  /**
   * Called when the session closes. This is a good time to release any external resource, for
   * example cancel a scheduled reloading task.
   */
  @Override
  void close();
}
