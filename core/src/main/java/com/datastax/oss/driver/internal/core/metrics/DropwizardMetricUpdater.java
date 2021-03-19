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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public abstract class DropwizardMetricUpdater<MetricT> extends AbstractMetricUpdater<MetricT> {

  private static final Logger LOG = LoggerFactory.getLogger(DropwizardMetricUpdater.class);

  protected final MetricRegistry registry;

  protected final ConcurrentMap<MetricT, Metric> metrics = new ConcurrentHashMap<>();

  protected final ConcurrentMap<MetricT, Reservoir> reservoirs = new ConcurrentHashMap<>();

  protected DropwizardMetricUpdater(
      InternalDriverContext context, Set<MetricT> enabledMetrics, MetricRegistry registry) {
    super(context, enabledMetrics);
    this.registry = registry;
  }

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  public <T extends Metric> T getMetric(
      MetricT metric, @SuppressWarnings("unused") String profileName) {
    return (T) metrics.get(metric);
  }

  @Override
  public void incrementCounter(MetricT metric, @Nullable String profileName, long amount) {
    if (isEnabled(metric, profileName)) {
      getOrCreateCounterFor(metric).inc(amount);
    }
  }

  @Override
  public void updateHistogram(MetricT metric, @Nullable String profileName, long value) {
    if (isEnabled(metric, profileName)) {
      getOrCreateHistogramFor(metric).update(value);
    }
  }

  @Override
  public void markMeter(MetricT metric, @Nullable String profileName, long amount) {
    if (isEnabled(metric, profileName)) {
      getOrCreateMeterFor(metric).mark(amount);
    }
  }

  @Override
  public void updateTimer(
      MetricT metric, @Nullable String profileName, long duration, TimeUnit unit) {
    if (isEnabled(metric, profileName)) {
      getOrCreateTimerFor(metric).update(duration, unit);
    }
  }

  @Override
  protected void clearMetrics() {
    for (MetricT metric : metrics.keySet()) {
      MetricId id = getMetricId(metric);
      registry.remove(id.getName());
    }
    metrics.clear();
    reservoirs.clear();
  }

  protected abstract MetricId getMetricId(MetricT metric);

  protected void initializeGauge(
      MetricT metric, DriverExecutionProfile profile, Supplier<Number> supplier) {
    if (isEnabled(metric, profile.getName())) {
      metrics.computeIfAbsent(
          metric,
          m -> {
            MetricId id = getMetricId(m);
            return registry.gauge(id.getName(), () -> supplier::get);
          });
    }
  }

  protected void initializeCounter(MetricT metric, DriverExecutionProfile profile) {
    if (isEnabled(metric, profile.getName())) {
      getOrCreateCounterFor(metric);
    }
  }

  protected void initializeHdrTimer(
      MetricT metric,
      DriverExecutionProfile profile,
      DriverOption highestLatency,
      DriverOption significantDigits,
      DriverOption interval) {
    if (isEnabled(metric, profile.getName())) {
      reservoirs.computeIfAbsent(
          metric, m -> createHdrReservoir(m, profile, highestLatency, significantDigits, interval));
      getOrCreateTimerFor(metric);
    }
  }

  protected Counter getOrCreateCounterFor(MetricT metric) {
    return (Counter)
        metrics.computeIfAbsent(
            metric,
            m -> {
              MetricId id = getMetricId(m);
              return registry.counter(id.getName());
            });
  }

  protected Meter getOrCreateMeterFor(MetricT metric) {
    return (Meter)
        metrics.computeIfAbsent(
            metric,
            m -> {
              MetricId id = getMetricId(m);
              return registry.meter(id.getName());
            });
  }

  protected Histogram getOrCreateHistogramFor(MetricT metric) {
    return (Histogram)
        metrics.computeIfAbsent(
            metric,
            m -> {
              MetricId id = getMetricId(m);
              return registry.histogram(id.getName());
            });
  }

  protected Timer getOrCreateTimerFor(MetricT metric) {
    return (Timer)
        metrics.computeIfAbsent(
            metric,
            m -> {
              MetricId id = getMetricId(m);
              Reservoir reservoir = reservoirs.get(metric);
              Timer timer = reservoir == null ? new Timer() : new Timer(reservoir);
              return registry.timer(id.getName(), () -> timer);
            });
  }

  protected HdrReservoir createHdrReservoir(
      MetricT metric,
      DriverExecutionProfile profile,
      DriverOption highestLatencyOption,
      DriverOption significantDigitsOption,
      DriverOption intervalOption) {
    MetricId id = getMetricId(metric);
    Duration highestLatency = profile.getDuration(highestLatencyOption);
    int significantDigits = profile.getInt(significantDigitsOption);
    if (significantDigits < 0 || significantDigits > 5) {
      LOG.warn(
          "[{}] Configuration option {} is out of range (expected between 0 and 5, "
              + "found {}); using 3 instead.",
          id.getName(),
          significantDigitsOption,
          significantDigits);
      significantDigits = 3;
    }
    Duration refreshInterval = profile.getDuration(intervalOption);
    return new HdrReservoir(highestLatency, significantDigits, refreshInterval, id.getName());
  }
}
