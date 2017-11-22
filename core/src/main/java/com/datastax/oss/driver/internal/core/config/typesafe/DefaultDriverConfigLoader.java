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

import com.datastax.oss.driver.api.core.ClusterBuilder;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.driver.internal.core.config.ForceReloadConfigEvent;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The default loader; it is based on TypeSafe Config and reloads at a configurable interval. */
public class DefaultDriverConfigLoader implements DriverConfigLoader {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDriverConfigLoader.class);

  public static final Supplier<Config> DEFAULT_CONFIG_SUPPLIER =
      () -> {
        ConfigFactory.invalidateCaches();
        return ConfigFactory.load().getConfig("datastax-java-driver");
      };

  private final Supplier<Config> configSupplier;
  private final TypeSafeDriverConfig driverConfig;

  private volatile SingleThreaded singleThreaded;

  /**
   * Builds a new instance with the default TypeSafe config loading rules (documented in {@link
   * ClusterBuilder#withConfigLoader(DriverConfigLoader)}) and the core driver options.
   */
  public DefaultDriverConfigLoader() {
    this(DEFAULT_CONFIG_SUPPLIER, CoreDriverOption.values());
  }

  /**
   * Builds an instance with custom arguments, if you want to load the configuration from somewhere
   * else or have custom options.
   */
  public DefaultDriverConfigLoader(Supplier<Config> configSupplier, DriverOption[]... options) {
    this.configSupplier = configSupplier;
    this.driverConfig = new TypeSafeDriverConfig(configSupplier.get(), options);
  }

  @Override
  public DriverConfig getInitialConfig() {
    return driverConfig;
  }

  @Override
  public void onDriverInit(DriverContext driverContext) {
    this.singleThreaded = new SingleThreaded((InternalDriverContext) driverContext);
  }

  @Override
  public void close() {
    SingleThreaded singleThreaded = this.singleThreaded;
    if (singleThreaded != null) {
      RunOrSchedule.on(singleThreaded.adminExecutor, singleThreaded::close);
    }
  }

  private class SingleThreaded {
    private final String logPrefix;
    private final EventExecutor adminExecutor;
    private final EventBus eventBus;
    private final DriverConfigProfile config;
    private final Object forceLoadListenerKey;

    private Duration reloadInterval;
    private ScheduledFuture<?> reloadFuture;
    private boolean closeWasCalled;

    private SingleThreaded(InternalDriverContext context) {
      this.logPrefix = context.clusterName();
      this.adminExecutor = context.nettyOptions().adminEventExecutorGroup().next();
      this.eventBus = context.eventBus();
      this.config = context.config().getDefaultProfile();
      this.reloadInterval =
          context.config().getDefaultProfile().getDuration(CoreDriverOption.CONFIG_RELOAD_INTERVAL);

      forceLoadListenerKey =
          this.eventBus.register(
              ForceReloadConfigEvent.class, e -> RunOrSchedule.on(adminExecutor, this::reload));

      RunOrSchedule.on(adminExecutor, this::scheduleReloadTask);
    }

    private void scheduleReloadTask() {
      assert adminExecutor.inEventLoop();
      // Cancel any previously running task
      if (reloadFuture != null) {
        reloadFuture.cancel(false);
      }
      if (reloadInterval.isZero()) {
        LOG.debug("[{}] Reload interval is 0, disabling periodic reloading", logPrefix);
      } else {
        LOG.debug("[{}] Scheduling periodic reloading with interval {}", logPrefix, reloadInterval);
        reloadFuture =
            adminExecutor.scheduleAtFixedRate(
                this::reload,
                reloadInterval.toNanos(),
                reloadInterval.toNanos(),
                TimeUnit.NANOSECONDS);
      }
    }

    private void reload() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      if (driverConfig.reload(configSupplier.get())) {
        LOG.info("[{}] Detected a configuration change", logPrefix);
        eventBus.fire(ConfigChangeEvent.INSTANCE);
        Duration newReloadInterval = config.getDuration(CoreDriverOption.CONFIG_RELOAD_INTERVAL);
        if (!newReloadInterval.equals(reloadInterval)) {
          reloadInterval = newReloadInterval;
          scheduleReloadTask();
        }
      } else {
        LOG.debug("[{}] Reloaded configuration but it hasn't changed", logPrefix);
      }
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      eventBus.unregister(forceLoadListenerKey, ForceReloadConfigEvent.class);
      if (reloadFuture != null) {
        reloadFuture.cancel(false);
      }
    }
  }
}
