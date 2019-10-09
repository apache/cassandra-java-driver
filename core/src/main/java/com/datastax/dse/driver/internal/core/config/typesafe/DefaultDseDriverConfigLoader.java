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
package com.datastax.dse.driver.internal.core.config.typesafe;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;

/**
 * The default loader for DSE; it is based on Typesafe Config and reloads at a configurable
 * interval.
 */
@ThreadSafe
public class DefaultDseDriverConfigLoader extends DefaultDriverConfigLoader {

  /**
   * This loads configuration files in the following order of descending priority.
   *
   * <ol>
   *   <li>1. System properties. e.g.
   *       -Ddatastax-java-driver.basic.load-balancing-policy.local-datacenter=dc1
   *   <li>2. The Application config, either specified by the system properties config.file,
   *       config.url, config.resource, or the default application.conf found in the system path.
   *   <li>3. The configuration values in the dse-reference.conf.
   *   <li>4. The configuration values in the reference.conf.
   * </ol>
   */
  private static final Supplier<Config> DEFAULT_DSE_CONFIG_SUPPLIER =
      () -> {
        ConfigFactory.invalidateCaches();
        Config config =
            ConfigFactory.defaultOverrides()
                .withFallback(ConfigFactory.defaultApplication())
                .withFallback(ConfigFactory.parseResourcesAnySyntax("dse-reference"))
                .withFallback(ConfigFactory.defaultReference())
                .resolve();
        return config.getConfig("datastax-java-driver");
      };

  public DefaultDseDriverConfigLoader() {
    this(DEFAULT_DSE_CONFIG_SUPPLIER);
  }

  /**
   * Builds an instance with custom arguments, if you want to load the configuration from somewhere
   * else.
   */
  public DefaultDseDriverConfigLoader(Supplier<Config> configSupplier) {
    super(configSupplier);
  }

  /**
   * Constructs a builder that may be used to provide additional configuration beyond those defined
   * in your configuration files programmatically. For example:
   *
   * <pre>{@code
   * DseSession session = DseSession.builder()
   *   .withConfigLoader(DefaultDseDriverConfigLoader.builder()
   *     .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(500))
   *     .build())
   *   .build();
   * }</pre>
   *
   * <p>In the general case, use of this is not recommended, but it may be useful in situations
   * where configuration must be defined at runtime or is derived from some other configuration
   * source.
   *
   * @deprecated Use {@link DriverConfigLoader#programmaticBuilder()} instead.
   */
  @NonNull
  @Deprecated
  public static com.datastax.oss.driver.internal.core.config.typesafe
          .DefaultDriverConfigLoaderBuilder
      builder() {
    return new com.datastax.oss.driver.internal.core.config.typesafe
        .DefaultDriverConfigLoaderBuilder() {
      @Override
      @NonNull
      public DriverConfigLoader build() {
        // fallback on the default config supplier (config file)
        return new DefaultDriverConfigLoader(
            () -> buildConfig().withFallback(DEFAULT_DSE_CONFIG_SUPPLIER.get()));
      }
    };
  }
}
