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
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public abstract class DropwizardMetricUpdater<MetricT> implements MetricUpdater<MetricT> {

  private static final Logger LOG = LoggerFactory.getLogger(DropwizardMetricUpdater.class);

  protected final Set<MetricT> enabledMetrics;
  protected final MetricRegistry registry;

  protected DropwizardMetricUpdater(Set<MetricT> enabledMetrics, MetricRegistry registry) {
    this.enabledMetrics = enabledMetrics;
    this.registry = registry;
  }

  protected abstract String buildFullName(MetricT metric);

  @Override
  public void incrementCounter(MetricT metric, long amount) {
    if (isEnabled(metric)) {
      registry.counter(buildFullName(metric)).inc(amount);
    }
  }

  @Override
  public void updateHistogram(MetricT metric, long value) {
    if (isEnabled(metric)) {
      registry.histogram(buildFullName(metric)).update(value);
    }
  }

  @Override
  public void markMeter(MetricT metric, long amount) {
    if (isEnabled(metric)) {
      registry.meter(buildFullName(metric)).mark(amount);
    }
  }

  @Override
  public void updateTimer(MetricT metric, long duration, TimeUnit unit) {
    if (isEnabled(metric)) {
      registry.timer(buildFullName(metric)).update(duration, unit);
    }
  }

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  public <T extends Metric> T getMetric(MetricT metric) {
    return (T) registry.getMetrics().get(buildFullName(metric));
  }

  @Override
  public boolean isEnabled(MetricT metric) {
    return enabledMetrics.contains(metric);
  }

  protected void initializeDefaultCounter(MetricT metric) {
    if (isEnabled(metric)) {
      // Just initialize eagerly so that the metric appears even when it has no data yet
      registry.counter(buildFullName(metric));
    }
  }

  protected void initializeHdrTimer(
      MetricT metric,
      DriverConfigProfile config,
      DefaultDriverOption highestLatencyOption,
      DefaultDriverOption significantDigitsOption,
      DefaultDriverOption intervalOption) {
    if (isEnabled(metric)) {
      String fullName = buildFullName(metric);

      Duration highestLatency = config.getDuration(highestLatencyOption);
      final int significantDigits;
      int d = config.getInt(significantDigitsOption);
      if (d >= 0 && d <= 5) {
        significantDigits = d;
      } else {
        LOG.warn(
            "[{}] Configuration option {} is out of range (expected between 0 and 5, found {}); "
                + "using 3 instead.",
            fullName,
            significantDigitsOption,
            d);
        significantDigits = 3;
      }
      Duration refreshInterval = config.getDuration(intervalOption);

      // Initialize eagerly to use the custom implementation
      registry.timer(
          fullName,
          () ->
              new Timer(
                  new HdrReservoir(highestLatency, significantDigits, refreshInterval, fullName)));
    }
  }
}
