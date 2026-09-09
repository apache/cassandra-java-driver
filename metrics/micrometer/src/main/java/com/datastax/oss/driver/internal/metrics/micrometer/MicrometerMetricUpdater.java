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
package com.datastax.oss.driver.internal.metrics.micrometer;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.AbstractMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.MetricId;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public abstract class MicrometerMetricUpdater<MetricT> extends AbstractMetricUpdater<MetricT> {

  protected final MeterRegistry registry;

  protected final ConcurrentMap<MetricT, Meter> metrics = new ConcurrentHashMap<>();

  protected MicrometerMetricUpdater(
      InternalDriverContext context, Set<MetricT> enabledMetrics, MeterRegistry registry) {
    super(context, enabledMetrics);
    this.registry = registry;
  }

  @Override
  public void incrementCounter(MetricT metric, @Nullable String profileName, long amount) {
    if (isEnabled(metric, profileName)) {
      getOrCreateCounterFor(metric).increment(amount);
    }
  }

  @Override
  public void updateHistogram(MetricT metric, @Nullable String profileName, long value) {
    if (isEnabled(metric, profileName)) {
      getOrCreateDistributionSummaryFor(metric).record(value);
    }
  }

  @Override
  public void markMeter(MetricT metric, @Nullable String profileName, long amount) {
    if (isEnabled(metric, profileName)) {
      // There is no meter type in Micrometer, so use a counter
      getOrCreateCounterFor(metric).increment(amount);
    }
  }

  @Override
  public void updateTimer(
      MetricT metric, @Nullable String profileName, long duration, TimeUnit unit) {
    if (isEnabled(metric, profileName)) {
      getOrCreateTimerFor(metric).record(duration, unit);
    }
  }

  @Override
  public void clearMetrics() {
    for (Meter metric : metrics.values()) {
      registry.remove(metric);
    }
    metrics.clear();
  }

  protected abstract MetricId getMetricId(MetricT metric);

  protected void initializeGauge(
      MetricT metric, DriverExecutionProfile profile, Supplier<Number> supplier) {
    if (isEnabled(metric, profile.getName())) {
      metrics.computeIfAbsent(
          metric,
          m -> {
            MetricId id = getMetricId(m);
            Iterable<Tag> tags = MicrometerTags.toMicrometerTags(id.getTags());
            return Gauge.builder(id.getName(), supplier).tags(tags).register(registry);
          });
    }
  }

  protected void initializeCounter(MetricT metric, DriverExecutionProfile profile) {
    if (isEnabled(metric, profile.getName())) {
      getOrCreateCounterFor(metric);
    }
  }

  protected void initializeTimer(MetricT metric, DriverExecutionProfile profile) {
    if (isEnabled(metric, profile.getName())) {
      getOrCreateTimerFor(metric);
    }
  }

  protected Counter getOrCreateCounterFor(MetricT metric) {
    return (Counter)
        metrics.computeIfAbsent(
            metric,
            m -> {
              MetricId id = getMetricId(m);
              Iterable<Tag> tags = MicrometerTags.toMicrometerTags(id.getTags());
              return Counter.builder(id.getName()).tags(tags).register(registry);
            });
  }

  protected DistributionSummary getOrCreateDistributionSummaryFor(MetricT metric) {
    return (DistributionSummary)
        metrics.computeIfAbsent(
            metric,
            m -> {
              MetricId id = getMetricId(m);
              Iterable<Tag> tags = MicrometerTags.toMicrometerTags(id.getTags());
              DistributionSummary.Builder builder =
                  DistributionSummary.builder(id.getName()).tags(tags);
              builder = configureDistributionSummary(builder, metric, id);
              return builder.register(registry);
            });
  }

  protected Timer getOrCreateTimerFor(MetricT metric) {
    return (Timer)
        metrics.computeIfAbsent(
            metric,
            m -> {
              MetricId id = getMetricId(m);
              Iterable<Tag> tags = MicrometerTags.toMicrometerTags(id.getTags());
              Timer.Builder builder = Timer.builder(id.getName()).tags(tags);
              builder = configureTimer(builder, metric, id);
              return builder.register(registry);
            });
  }

  protected Timer.Builder configureTimer(Timer.Builder builder, MetricT metric, MetricId id) {
    DriverExecutionProfile profile = context.getConfig().getDefaultProfile();
    if (profile.getBoolean(DefaultDriverOption.METRICS_GENERATE_AGGREGABLE_HISTOGRAMS)) {
      builder.publishPercentileHistogram();
    }
    if (profile.isDefined(DefaultDriverOption.METRICS_HISTOGRAM_PUBLISH_LOCAL_PERCENTILES)) {
      builder.publishPercentiles(
          toDoubleArray(
              profile.getDoubleList(
                  DefaultDriverOption.METRICS_HISTOGRAM_PUBLISH_LOCAL_PERCENTILES)));
    }
    return builder;
  }

  @SuppressWarnings("unused")
  protected DistributionSummary.Builder configureDistributionSummary(
      DistributionSummary.Builder builder, MetricT metric, MetricId id) {
    DriverExecutionProfile profile = context.getConfig().getDefaultProfile();
    if (profile.getBoolean(DefaultDriverOption.METRICS_GENERATE_AGGREGABLE_HISTOGRAMS)) {
      builder.publishPercentileHistogram();
    }
    return builder;
  }

  static double[] toDoubleArray(List<Double> doubleList) {
    return doubleList.stream().mapToDouble(Double::doubleValue).toArray();
  }
}
