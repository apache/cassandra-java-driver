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
package com.datastax.oss.driver.internal.metrics.micrometer;

import com.datastax.dse.driver.api.core.metrics.DseNodeMetric;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class MicrometerNodeMetricUpdater extends MicrometerMetricUpdater<NodeMetric>
    implements NodeMetricUpdater {

  private final String metricNamePrefix;
  private final Runnable signalMetricUpdated;

  public MicrometerNodeMetricUpdater(
      Node node,
      Set<NodeMetric> enabledMetrics,
      MeterRegistry registry,
      DriverContext driverContext,
      Runnable signalMetricUpdated) {
    super(enabledMetrics, registry);
    this.signalMetricUpdated = signalMetricUpdated;
    InternalDriverContext context = (InternalDriverContext) driverContext;
    this.metricNamePrefix = buildPrefix(driverContext.getSessionName(), node.getEndPoint());

    DriverExecutionProfile config = driverContext.getConfig().getDefaultProfile();

    if (enabledMetrics.contains(DefaultNodeMetric.OPEN_CONNECTIONS)) {
      this.registry.gauge(
          buildFullName(DefaultNodeMetric.OPEN_CONNECTIONS, null), node.getOpenConnections());
    }
    initializePoolGauge(
        DefaultNodeMetric.AVAILABLE_STREAMS, node, ChannelPool::getAvailableIds, context);
    initializePoolGauge(DefaultNodeMetric.IN_FLIGHT, node, ChannelPool::getInFlight, context);
    initializePoolGauge(
        DefaultNodeMetric.ORPHANED_STREAMS, node, ChannelPool::getOrphanedIds, context);
    initializeTimer(DefaultNodeMetric.CQL_MESSAGES, config);
    initializeDefaultCounter(DefaultNodeMetric.UNSENT_REQUESTS, null);
    initializeDefaultCounter(DefaultNodeMetric.ABORTED_REQUESTS, null);
    initializeDefaultCounter(DefaultNodeMetric.WRITE_TIMEOUTS, null);
    initializeDefaultCounter(DefaultNodeMetric.READ_TIMEOUTS, null);
    initializeDefaultCounter(DefaultNodeMetric.UNAVAILABLES, null);
    initializeDefaultCounter(DefaultNodeMetric.OTHER_ERRORS, null);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES, null);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES_ON_ABORTED, null);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES_ON_READ_TIMEOUT, null);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES_ON_WRITE_TIMEOUT, null);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES_ON_UNAVAILABLE, null);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES_ON_OTHER_ERROR, null);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES, null);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES_ON_ABORTED, null);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES_ON_READ_TIMEOUT, null);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES_ON_WRITE_TIMEOUT, null);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES_ON_UNAVAILABLE, null);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES_ON_OTHER_ERROR, null);
    initializeDefaultCounter(DefaultNodeMetric.SPECULATIVE_EXECUTIONS, null);
    initializeDefaultCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);
    initializeDefaultCounter(DefaultNodeMetric.AUTHENTICATION_ERRORS, null);
    initializeTimer(DseNodeMetric.GRAPH_MESSAGES, driverContext.getConfig().getDefaultProfile());
  }

  @Override
  public String buildFullName(NodeMetric metric, String profileName) {
    return metricNamePrefix + metric.getPath();
  }

  private String buildPrefix(String sessionName, EndPoint endPoint) {
    return sessionName + ".nodes." + endPoint.asMetricPrefix() + ".";
  }

  @Override
  public void incrementCounter(NodeMetric metric, String profileName, long amount) {
    signalMetricUpdated.run();
    super.incrementCounter(metric, profileName, amount);
  }

  @Override
  public void updateHistogram(NodeMetric metric, String profileName, long value) {
    signalMetricUpdated.run();
    super.updateHistogram(metric, profileName, value);
  }

  @Override
  public void markMeter(NodeMetric metric, String profileName, long amount) {
    signalMetricUpdated.run();
    super.markMeter(metric, profileName, amount);
  }

  @Override
  public void updateTimer(NodeMetric metric, String profileName, long duration, TimeUnit unit) {
    signalMetricUpdated.run();
    super.updateTimer(metric, profileName, duration, unit);
  }

  private void initializePoolGauge(
      NodeMetric metric,
      Node node,
      Function<ChannelPool, Integer> reading,
      InternalDriverContext context) {
    if (enabledMetrics.contains(metric)) {
      final String metricName = buildFullName(metric, null);
      registry.gauge(
          metricName,
          context,
          c -> {
            ChannelPool pool = c.getPoolManager().getPools().get(node);
            return (pool == null) ? 0 : reading.apply(pool);
          });
    }
  }

  public void cleanupNodeMetrics() {
    registry.getMeters().removeIf(metric -> metric.getId().getName().startsWith(metricNamePrefix));
  }
}
