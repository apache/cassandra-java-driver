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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default loader; it is based on Typesafe Config and optionally reloads at a configurable
 * interval.
 */
@ThreadSafe
public class DefaultDriverConfigLoader implements DriverConfigLoader {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDriverConfigLoader.class);

  public static final String DEFAULT_ROOT_PATH = "datastax-java-driver";

  public static final Supplier<Config> DEFAULT_CONFIG_SUPPLIER =
      () -> {
        ConfigFactory.invalidateCaches();
        // The thread's context class loader will be used for application classpath resources,
        // while the driver class loader will be used for reference classpath resources.
        return ConfigFactory.defaultOverrides()
            .withFallback(ConfigFactory.defaultApplication())
            .withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader()))
            .resolve()
            .getConfig(DEFAULT_ROOT_PATH);
      };

  @NonNull
  public static DefaultDriverConfigLoader fromClasspath(
      @NonNull String resourceBaseName, @NonNull ClassLoader appClassLoader) {
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(
                      ConfigFactory.parseResourcesAnySyntax(
                          resourceBaseName,
                          ConfigParseOptions.defaults().setClassLoader(appClassLoader)))
                  .withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader()))
                  .resolve();
          return config.getConfig(DEFAULT_ROOT_PATH);
        });
  }

  @NonNull
  public static DriverConfigLoader fromFile(@NonNull File file) {
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(ConfigFactory.parseFileAnySyntax(file))
                  .withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader()))
                  .resolve();
          return config.getConfig(DEFAULT_ROOT_PATH);
        });
  }

  @NonNull
  public static DriverConfigLoader fromUrl(@NonNull URL url) {
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(ConfigFactory.parseURL(url))
                  .withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader()))
                  .resolve();
          return config.getConfig(DEFAULT_ROOT_PATH);
        });
  }

  @NonNull
  public static DefaultDriverConfigLoader fromString(@NonNull String contents) {
    return new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config config =
              ConfigFactory.defaultOverrides()
                  .withFallback(ConfigFactory.parseString(contents))
                  .withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader()))
                  .resolve();
          return config.getConfig(DEFAULT_ROOT_PATH);
        },
        false);
  }

  private final Supplier<Config> configSupplier;
  private final TypesafeDriverConfig driverConfig;
  private final boolean supportsReloading;

  private volatile SingleThreaded singleThreaded;

  /**
   * Builds a new instance with the default Typesafe config loading rules (documented in {@link
   * SessionBuilder#withConfigLoader(DriverConfigLoader)}) and the core driver options. This
   * constructor enables config reloading (that is, {@link #supportsReloading} will return true).
   *
   * <p>Application-specific classpath resources will be located using the {@linkplain
   * Thread#getContextClassLoader() the current thread's context class loader}. This might not be
   * suitable for OSGi deployments, which should use {@link #DefaultDriverConfigLoader(ClassLoader)}
   * instead.
   */
  public DefaultDriverConfigLoader() {
    this(DEFAULT_CONFIG_SUPPLIER);
  }

  /**
   * Builds a new instance with the default Typesafe config loading rules (documented in {@link
   * SessionBuilder#withConfigLoader(DriverConfigLoader)}) and the core driver options, except that
   * application-specific classpath resources will be located using the provided {@link ClassLoader}
   * instead of {@linkplain Thread#getContextClassLoader() the current thread's context class
   * loader}. This constructor enables config reloading (that is, {@link #supportsReloading} will
   * return true).
   */
  public DefaultDriverConfigLoader(@NonNull ClassLoader appClassLoader) {
    this(
        () -> {
          ConfigFactory.invalidateCaches();
          return ConfigFactory.defaultOverrides()
              .withFallback(ConfigFactory.defaultApplication(appClassLoader))
              .withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader()))
              .resolve()
              .getConfig(DEFAULT_ROOT_PATH);
        });
  }

  /**
   * Builds an instance with custom arguments, if you want to load the configuration from somewhere
   * else. This constructor enables config reloading (that is, {@link #supportsReloading} will
   * return true).
   *
   * @param configSupplier A supplier for the Typesafe {@link Config}; it will be invoked once when
   *     this object is instantiated, and at each reload attempt, if reloading is enabled.
   */
  public DefaultDriverConfigLoader(@NonNull Supplier<Config> configSupplier) {
    this(configSupplier, true);
  }

  /**
   * Builds an instance with custom arguments, if you want to load the configuration from somewhere
   * else and/or modify config reload behavior.
   *
   * @param configSupplier A supplier for the Typesafe {@link Config}; it will be invoked once when
   *     this object is instantiated, and at each reload attempt, if reloading is enabled.
   * @param supportsReloading Whether config reloading should be enabled or not.
   */
  public DefaultDriverConfigLoader(
      @NonNull Supplier<Config> configSupplier, boolean supportsReloading) {
    this.configSupplier = configSupplier;
    this.driverConfig = new TypesafeDriverConfig(configSupplier.get());
    this.supportsReloading = supportsReloading;
  }

  @NonNull
  @Override
  public DriverConfig getInitialConfig() {
    return driverConfig;
  }

  @Override
  public void onDriverInit(@NonNull DriverContext driverContext) {
    this.singleThreaded = new SingleThreaded((InternalDriverContext) driverContext);
  }

  @NonNull
  @Override
  public final CompletionStage<Boolean> reload() {
    if (supportsReloading) {
      CompletableFuture<Boolean> result = new CompletableFuture<>();
      RunOrSchedule.on(singleThreaded.adminExecutor, () -> singleThreaded.reload(result));
      return result;
    } else {
      return CompletableFutures.failedFuture(
          new UnsupportedOperationException(
              "This instance of DefaultDriverConfigLoader does not support reloading"));
    }
  }

  @Override
  public final boolean supportsReloading() {
    return supportsReloading;
  }

  /** For internal use only, this leaks a Typesafe config type. */
  @NonNull
  public Supplier<Config> getConfigSupplier() {
    return configSupplier;
  }

  @Override
  public void close() {
    SingleThreaded singleThreaded = this.singleThreaded;
    if (singleThreaded != null && !singleThreaded.adminExecutor.terminationFuture().isDone()) {
      try {
        RunOrSchedule.on(singleThreaded.adminExecutor, singleThreaded::close);
      } catch (RejectedExecutionException e) {
        // Checking the future is racy, there is still a tiny window that could get us here.
        // We can safely ignore this error because, if the execution is rejected, the periodic
        // reload task, if any, has been already cancelled.
      }
    }
  }

  /**
   * Constructs a builder that may be used to provide additional configuration beyond those defined
   * in your configuration files programmatically. For example:
   *
   * <pre>{@code
   * CqlSession session = CqlSession.builder()
   *   .withConfigLoader(DefaultDriverConfigLoader.builder()
   *     .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(500))
   *     .build())
   *   .build();
   * }</pre>
   *
   * <p>In the general case, use of this is not recommended, but it may be useful in situations
   * where configuration must be defined at runtime or is derived from some other configuration
   * source.
   *
   * @deprecated this feature is now available in the public API. Use {@link
   *     DriverConfigLoader#programmaticBuilder()} instead.
   */
  @Deprecated
  @NonNull
  public static DefaultDriverConfigLoaderBuilder builder() {
    return new DefaultDriverConfigLoaderBuilder();
  }

  private class SingleThreaded {
    private final String logPrefix;
    private final EventExecutor adminExecutor;
    private final EventBus eventBus;
    private final DriverExecutionProfile config;

    private Duration reloadInterval;
    private ScheduledFuture<?> periodicTaskHandle;
    private boolean closeWasCalled;

    private SingleThreaded(InternalDriverContext context) {
      this.logPrefix = context.getSessionName();
      this.adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
      this.eventBus = context.getEventBus();
      this.config = context.getConfig().getDefaultProfile();
      this.reloadInterval =
          context
              .getConfig()
              .getDefaultProfile()
              .getDuration(DefaultDriverOption.CONFIG_RELOAD_INTERVAL);

      RunOrSchedule.on(adminExecutor, this::schedulePeriodicReload);
    }

    private void schedulePeriodicReload() {
      assert adminExecutor.inEventLoop();
      // Cancel any previously running task
      if (periodicTaskHandle != null) {
        periodicTaskHandle.cancel(false);
      }
      if (reloadInterval.isZero()) {
        LOG.debug("[{}] Reload interval is 0, disabling periodic reloading", logPrefix);
      } else {
        LOG.debug("[{}] Scheduling periodic reloading with interval {}", logPrefix, reloadInterval);
        periodicTaskHandle =
            adminExecutor.scheduleAtFixedRate(
                this::reloadInBackground,
                reloadInterval.toNanos(),
                reloadInterval.toNanos(),
                TimeUnit.NANOSECONDS);
      }
    }

    /**
     * @param reloadedFuture a future to complete when the reload is complete (might be null if the
     *     caller is not interested in being notified)
     */
    private void reload(CompletableFuture<Boolean> reloadedFuture) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        if (reloadedFuture != null) {
          reloadedFuture.completeExceptionally(new IllegalStateException("session is closing"));
        }
        return;
      }
      try {
        boolean changed = driverConfig.reload(configSupplier.get());
        if (changed) {
          LOG.info("[{}] Detected a configuration change", logPrefix);
          eventBus.fire(ConfigChangeEvent.INSTANCE);
          Duration newReloadInterval =
              config.getDuration(DefaultDriverOption.CONFIG_RELOAD_INTERVAL);
          if (!newReloadInterval.equals(reloadInterval)) {
            reloadInterval = newReloadInterval;
            schedulePeriodicReload();
          }
        } else {
          LOG.debug("[{}] Reloaded configuration but it hasn't changed", logPrefix);
        }
        if (reloadedFuture != null) {
          reloadedFuture.complete(changed);
        }
      } catch (Error | RuntimeException e) {
        if (reloadedFuture != null) {
          reloadedFuture.completeExceptionally(e);
        } else {
          Loggers.warnWithException(
              LOG, "[{}] Unexpected exception during scheduled reload", logPrefix, e);
        }
      }
    }

    private void reloadInBackground() {
      reload(null);
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      if (periodicTaskHandle != null) {
        periodicTaskHandle.cancel(false);
      }
    }
  }
}
