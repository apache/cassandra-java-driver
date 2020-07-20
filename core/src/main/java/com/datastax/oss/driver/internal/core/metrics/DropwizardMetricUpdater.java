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
package com.datastax.oss.driver.internal.core.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.shaded.guava.common.base.Ticker;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.RemovalNotification;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.UncheckedExecutionException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public abstract class DropwizardMetricUpdater<MetricT> implements MetricUpdater<MetricT> {

  private static final Logger LOG = LoggerFactory.getLogger(DropwizardMetricUpdater.class);

  protected final Set<MetricT> enabledMetrics;

  protected final MetricRegistry registry;

  protected final Cache<String, Metric> metricsCache;

  protected DropwizardMetricUpdater(
      Set<MetricT> enabledMetrics, MetricRegistry registry, Ticker ticker) {
    this(enabledMetrics, registry, ticker, Duration.ofHours(1));
  }

  protected DropwizardMetricUpdater(
      Set<MetricT> enabledMetrics, MetricRegistry registry, Ticker ticker, Duration expiresAfter) {
    this.enabledMetrics = enabledMetrics;
    this.registry = registry;
    metricsCache =
        CacheBuilder.newBuilder()
            .ticker(ticker)
            .expireAfterAccess(expiresAfter)
            .removalListener(
                (RemovalNotification<String, Metric> notif) -> {
                  LOG.debug("Removing {} from cache after {}", notif.getKey(), expiresAfter);
                  registry.remove(notif.getKey());
                })
            .build();
  }

  protected abstract String buildFullName(MetricT metric, String profileName);

  @Override
  public void incrementCounter(MetricT metric, String profileName, long amount) {
    if (isEnabled(metric, profileName)) {
      getOrCreateMetric(metric, profileName, registry::counter).inc();
    }
  }

  @Override
  public void updateHistogram(MetricT metric, String profileName, long value) {
    if (isEnabled(metric, profileName)) {
      getOrCreateMetric(metric, profileName, registry::histogram).update(value);
    }
  }

  @Override
  public void markMeter(MetricT metric, String profileName, long amount) {
    if (isEnabled(metric, profileName)) {
      getOrCreateMetric(metric, profileName, registry::meter).mark(amount);
    }
  }

  @Override
  public void updateTimer(MetricT metric, String profileName, long duration, TimeUnit unit) {
    if (isEnabled(metric, profileName)) {
      getOrCreateMetric(metric, profileName, registry::timer).update(duration, unit);
    }
  }

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  public <T extends Metric> T getMetric(MetricT metric, String profileName) {
    String name = buildFullName(metric, profileName);
    return (T) metricsCache.getIfPresent(name);
  }

  @SuppressWarnings("unchecked")
  protected <T extends Metric> T getOrCreateMetric(
      MetricT metric, String profileName, Function<String, T> metricBuilder) {
    String name = buildFullName(metric, profileName);
    try {
      return (T) metricsCache.get(name, () -> metricBuilder.apply(name));
    } catch (ExecutionException ignored) {
      // should never happen
      throw new AssertionError();
    } catch (UncheckedExecutionException e) {
      throw (RuntimeException) e.getCause();
    }
  }

  @Override
  public boolean isEnabled(MetricT metric, String profileName) {
    return enabledMetrics.contains(metric);
  }

  protected void initializeDefaultCounter(MetricT metric, String profileName) {
    if (isEnabled(metric, profileName)) {
      // Just initialize eagerly so that the metric appears even when it has no data yet
      getOrCreateMetric(metric, profileName, registry::counter);
    }
  }

  protected void initializeHdrTimer(
      MetricT metric,
      DriverExecutionProfile config,
      DriverOption highestLatencyOption,
      DriverOption significantDigitsOption,
      DriverOption intervalOption) {
    String profileName = config.getName();
    if (isEnabled(metric, profileName)) {
      // Initialize eagerly to use the custom implementation
      getOrCreateMetric(
          metric,
          profileName,
          metricName -> {
            Duration highestLatency = config.getDuration(highestLatencyOption);
            final int significantDigits;
            int d = config.getInt(significantDigitsOption);
            if (d >= 0 && d <= 5) {
              significantDigits = d;
            } else {
              LOG.warn(
                  "[{}] Configuration option {} is out of range (expected between 0 and 5, found {}); "
                      + "using 3 instead.",
                  metricName,
                  significantDigitsOption,
                  d);
              significantDigits = 3;
            }
            Duration refreshInterval = config.getDuration(intervalOption);
            HdrReservoir reservoir =
                new HdrReservoir(highestLatency, significantDigits, refreshInterval, metricName);
            return registry.register(metricName, new Timer(reservoir));
          });
    }
  }
}
