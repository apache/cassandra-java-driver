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
package com.datastax.oss.driver.internal.metrics.microprofile;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.metrics.MetricUpdater;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.eclipse.microprofile.metrics.MetricRegistry;

@ThreadSafe
public abstract class MicroProfileMetricUpdater<MetricT> implements MetricUpdater<MetricT> {

  protected final Set<MetricT> enabledMetrics;
  protected final MetricRegistry registry;

  protected MicroProfileMetricUpdater(Set<MetricT> enabledMetrics, MetricRegistry registry) {
    this.enabledMetrics = enabledMetrics;
    this.registry = registry;
  }

  protected abstract String buildFullName(MetricT metric, String profileName);

  @Override
  public void incrementCounter(MetricT metric, String profileName, long amount) {
    if (isEnabled(metric, profileName)) {
      registry.counter(buildFullName(metric, profileName)).inc(amount);
    }
  }

  @Override
  public void updateHistogram(MetricT metric, String profileName, long value) {
    if (isEnabled(metric, profileName)) {
      registry.histogram(buildFullName(metric, profileName)).update(value);
    }
  }

  @Override
  public void markMeter(MetricT metric, String profileName, long amount) {
    if (isEnabled(metric, profileName)) {
      registry.meter(buildFullName(metric, profileName)).mark(amount);
    }
  }

  @Override
  public void updateTimer(MetricT metric, String profileName, long duration, TimeUnit unit) {
    if (isEnabled(metric, profileName)) {
      registry.timer(buildFullName(metric, profileName)).update(duration, unit);
    }
  }

  @Override
  public boolean isEnabled(MetricT metric, String profileName) {
    return enabledMetrics.contains(metric);
  }

  protected void initializeDefaultCounter(MetricT metric, String profileName) {
    if (isEnabled(metric, profileName)) {
      // Just initialize eagerly so that the metric appears even when it has no data yet
      registry.counter(buildFullName(metric, profileName));
    }
  }

  protected void initializeTimer(MetricT metric, DriverExecutionProfile config) {
    String profileName = config.getName();
    if (isEnabled(metric, profileName)) {
      String fullName = buildFullName(metric, profileName);

      registry.timer(fullName);
    }
  }
}
