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
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
   * Builds an instance using the driver's default implementation (based on Typesafe config), except
   * that application-specific options are loaded from a classpath resource with a custom name.
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
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(ConfigFactory.parseResourcesAnySyntax(resourceBaseName))
                  .withFallback(ConfigFactory.defaultReference())
                  .resolve();
          return config.getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
        });
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
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(ConfigFactory.parseFileAnySyntax(file))
                  .withFallback(ConfigFactory.defaultReference())
                  .resolve();
          return config.getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
        });
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
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(ConfigFactory.parseURL(url))
                  .withFallback(ConfigFactory.defaultReference())
                  .resolve();
          return config.getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
        });
  }

  /**
   * Starts a builder that allows configuration options to be overridden programmatically.
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
   * <p>The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>The loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   *
   * <p>Note that the returned builder is <b>not thread-safe</b>.
   */
  @NonNull
  static ProgrammaticDriverConfigLoaderBuilder programmaticBuilder() {
    return new DefaultProgrammaticDriverConfigLoaderBuilder();
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
   * Called when the cluster closes. This is a good time to release any external resource, for
   * example cancel a scheduled reloading task.
   */
  @Override
  void close();
}
