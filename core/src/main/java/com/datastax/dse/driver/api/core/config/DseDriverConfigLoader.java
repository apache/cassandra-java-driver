/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.config;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.File;
import java.net.URL;

/**
 * Exposes factory methods to create config loaders from the DSE driver.
 *
 * <p>Note that this class only exists to expose those methods in a way that is symmetric to its OSS
 * counterpart {@link DriverConfigLoader}. It does not extend it, DSE-specific loaders are regular
 * instances of the OSS type.
 */
public class DseDriverConfigLoader {

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
   *   <li>{@code dse-reference.conf} (all resources on classpath with this name). In particular,
   *       this will load the {@code dse-reference.conf} included in the core DSE driver JAR, that
   *       defines default options for all DSE-specific mandatory options.
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core OSS driver JAR, that defines
   *       default options for all mandatory options common to OSS and DSE.
   * </ul>
   *
   * The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>The returned loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   */
  @NonNull
  public static DriverConfigLoader fromClasspath(@NonNull String resourceBaseName) {
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(ConfigFactory.parseResourcesAnySyntax(resourceBaseName))
                  .withFallback(ConfigFactory.parseResourcesAnySyntax("dse-reference"))
                  .withFallback(ConfigFactory.defaultReference())
                  .resolve();
          return config.getConfig("datastax-java-driver");
        });
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
   *   <li>{@code dse-reference.conf} (all resources on classpath with this name). In particular,
   *       this will load the {@code dse-reference.conf} included in the core DSE driver JAR, that
   *       defines default options for all DSE-specific mandatory options.
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core OSS driver JAR, that defines
   *       default options for all mandatory options common to OSS and DSE.
   * </ul>
   *
   * The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>The returned loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   */
  @NonNull
  public static DriverConfigLoader fromFile(@NonNull File file) {
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(ConfigFactory.parseFileAnySyntax(file))
                  .withFallback(ConfigFactory.parseResourcesAnySyntax("dse-reference"))
                  .withFallback(ConfigFactory.defaultReference())
                  .resolve();
          return config.getConfig("datastax-java-driver");
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
   *   <li>{@code dse-reference.conf} (all resources on classpath with this name). In particular,
   *       this will load the {@code dse-reference.conf} included in the core DSE driver JAR, that
   *       defines default options for all DSE-specific mandatory options.
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core OSS driver JAR, that defines
   *       default options for all mandatory options common to OSS and DSE.
   * </ul>
   *
   * The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>The returned loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   */
  @NonNull
  public static DriverConfigLoader fromUrl(@NonNull URL url) {
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(ConfigFactory.parseURL(url))
                  .withFallback(ConfigFactory.parseResourcesAnySyntax("dse-reference"))
                  .withFallback(ConfigFactory.defaultReference())
                  .resolve();
          return config.getConfig("datastax-java-driver");
        });
  }

  /**
   * Starts a builder that allows configuration options to be overridden programmatically.
   *
   * <p>Sample usage:
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
   *   <li>{@code dse-reference.conf} (all resources on classpath with this name). In particular,
   *       this will load the {@code dse-reference.conf} included in the core DSE driver JAR, that
   *       defines default options for all DSE-specific mandatory options.
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core OSS driver JAR, that defines
   *       default options for all mandatory options common to OSS and DSE.
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
  public static ProgrammaticDriverConfigLoaderBuilder programmaticBuilder() {
    return new DefaultProgrammaticDriverConfigLoaderBuilder(
        () ->
            ConfigFactory.defaultApplication()
                .withFallback(ConfigFactory.parseResourcesAnySyntax("dse-reference"))
                .withFallback(ConfigFactory.defaultReference()),
        DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
  }

  private DseDriverConfigLoader() {
    throw new AssertionError("Not meant to be instantiated");
  }
}
