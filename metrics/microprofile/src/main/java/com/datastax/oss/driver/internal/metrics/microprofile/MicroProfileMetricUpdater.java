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
package com.datastax.oss.driver.internal.metrics.microprofile;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.AbstractMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.MetricId;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.Gauge;
import org.eclipse.microprofile.metrics.Histogram;
import org.eclipse.microprofile.metrics.Metadata;
import org.eclipse.microprofile.metrics.Meter;
import org.eclipse.microprofile.metrics.Metric;
import org.eclipse.microprofile.metrics.MetricID;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.MetricType;
import org.eclipse.microprofile.metrics.Tag;
import org.eclipse.microprofile.metrics.Timer;

@ThreadSafe
public abstract class MicroProfileMetricUpdater<MetricT> extends AbstractMetricUpdater<MetricT> {

  protected final MetricRegistry registry;

  protected final ConcurrentMap<MetricT, Metric> metrics = new ConcurrentHashMap<>();

  protected MicroProfileMetricUpdater(
      InternalDriverContext context, Set<MetricT> enabledMetrics, MetricRegistry registry) {
    super(context, enabledMetrics);
    this.registry = registry;
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
      getOrCreateTimerFor(metric).update(Duration.ofNanos(unit.toNanos(duration)));
    }
  }

  @Override
  protected void clearMetrics() {
    for (MetricT metric : metrics.keySet()) {
      MetricId id = getMetricId(metric);
      Tag[] tags = MicroProfileTags.toMicroProfileTags(id.getTags());
      registry.remove(new MetricID(id.getName(), tags));
    }
    metrics.clear();
  }

  protected abstract MetricId getMetricId(MetricT metric);

  protected void initializeGauge(
      MetricT metric, DriverExecutionProfile profile, Gauge<Number> supplier) {
    if (isEnabled(metric, profile.getName())) {
      metrics.computeIfAbsent(
          metric,
          m -> {
            MetricId id = getMetricId(m);
            String name = id.getName();
            Tag[] tags = MicroProfileTags.toMicroProfileTags(id.getTags());
            Metadata metadata =
                Metadata.builder().withName(name).withType(MetricType.GAUGE).build();
            return registry.register(metadata, supplier, tags);
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
              Tag[] tags = MicroProfileTags.toMicroProfileTags(id.getTags());
              return registry.counter(id.getName(), tags);
            });
  }

  protected Meter getOrCreateMeterFor(MetricT metric) {
    return (Meter)
        metrics.computeIfAbsent(
            metric,
            m -> {
              MetricId id = getMetricId(m);
              Tag[] tags = MicroProfileTags.toMicroProfileTags(id.getTags());
              return registry.meter(id.getName(), tags);
            });
  }

  protected Histogram getOrCreateHistogramFor(MetricT metric) {
    return (Histogram)
        metrics.computeIfAbsent(
            metric,
            m -> {
              MetricId id = getMetricId(m);
              Tag[] tags = MicroProfileTags.toMicroProfileTags(id.getTags());
              return registry.histogram(id.getName(), tags);
            });
  }

  protected Timer getOrCreateTimerFor(MetricT metric) {
    return (Timer)
        metrics.computeIfAbsent(
            metric,
            m -> {
              MetricId id = getMetricId(m);
              Tag[] tags = MicroProfileTags.toMicroProfileTags(id.getTags());
              return registry.timer(id.getName(), tags);
            });
  }
}
