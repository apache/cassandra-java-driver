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

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.metrics.DseNodeMetric;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricId;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class MicrometerNodeMetricUpdater extends MicrometerMetricUpdater<NodeMetric>
    implements NodeMetricUpdater {

  private final Node node;

  public MicrometerNodeMetricUpdater(
      Node node,
      InternalDriverContext context,
      Set<NodeMetric> enabledMetrics,
      MeterRegistry registry) {
    super(context, enabledMetrics, registry);
    this.node = node;

    DriverExecutionProfile profile = context.getConfig().getDefaultProfile();

    initializeGauge(DefaultNodeMetric.OPEN_CONNECTIONS, profile, node::getOpenConnections);
    initializeGauge(DefaultNodeMetric.AVAILABLE_STREAMS, profile, () -> availableStreamIds(node));
    initializeGauge(DefaultNodeMetric.IN_FLIGHT, profile, () -> inFlightRequests(node));
    initializeGauge(DefaultNodeMetric.ORPHANED_STREAMS, profile, () -> orphanedStreamIds(node));

    initializeCounter(DefaultNodeMetric.UNSENT_REQUESTS, profile);
    initializeCounter(DefaultNodeMetric.ABORTED_REQUESTS, profile);
    initializeCounter(DefaultNodeMetric.WRITE_TIMEOUTS, profile);
    initializeCounter(DefaultNodeMetric.READ_TIMEOUTS, profile);
    initializeCounter(DefaultNodeMetric.UNAVAILABLES, profile);
    initializeCounter(DefaultNodeMetric.OTHER_ERRORS, profile);
    initializeCounter(DefaultNodeMetric.RETRIES, profile);
    initializeCounter(DefaultNodeMetric.RETRIES_ON_ABORTED, profile);
    initializeCounter(DefaultNodeMetric.RETRIES_ON_READ_TIMEOUT, profile);
    initializeCounter(DefaultNodeMetric.RETRIES_ON_WRITE_TIMEOUT, profile);
    initializeCounter(DefaultNodeMetric.RETRIES_ON_UNAVAILABLE, profile);
    initializeCounter(DefaultNodeMetric.RETRIES_ON_OTHER_ERROR, profile);
    initializeCounter(DefaultNodeMetric.IGNORES, profile);
    initializeCounter(DefaultNodeMetric.IGNORES_ON_ABORTED, profile);
    initializeCounter(DefaultNodeMetric.IGNORES_ON_READ_TIMEOUT, profile);
    initializeCounter(DefaultNodeMetric.IGNORES_ON_WRITE_TIMEOUT, profile);
    initializeCounter(DefaultNodeMetric.IGNORES_ON_UNAVAILABLE, profile);
    initializeCounter(DefaultNodeMetric.IGNORES_ON_OTHER_ERROR, profile);
    initializeCounter(DefaultNodeMetric.SPECULATIVE_EXECUTIONS, profile);
    initializeCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, profile);
    initializeCounter(DefaultNodeMetric.AUTHENTICATION_ERRORS, profile);

    initializeTimer(DefaultNodeMetric.CQL_MESSAGES, profile);
    initializeTimer(DseNodeMetric.GRAPH_MESSAGES, profile);
  }

  @Override
  protected MetricId getMetricId(NodeMetric metric) {
    return context.getMetricIdGenerator().nodeMetricId(node, metric);
  }

  @Override
  protected void startMetricsExpirationTimeout() {
    super.startMetricsExpirationTimeout();
  }

  @Override
  protected void cancelMetricsExpirationTimeout() {
    super.cancelMetricsExpirationTimeout();
  }

  @Override
  protected Timer.Builder configureTimer(Timer.Builder builder, NodeMetric metric, MetricId id) {
    DriverExecutionProfile profile = context.getConfig().getDefaultProfile();
    super.configureTimer(builder, metric, id);
    if (metric == DefaultNodeMetric.CQL_MESSAGES) {
      builder
          .minimumExpectedValue(
              profile.getDuration(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_LOWEST))
          .maximumExpectedValue(
              profile.getDuration(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST))
          .serviceLevelObjectives(
              profile.isDefined(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_SLO)
                  ? profile
                      .getDurationList(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_SLO)
                      .toArray(new Duration[0])
                  : null)
          .percentilePrecision(
              profile.getInt(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS));

    } else if (metric == DseNodeMetric.GRAPH_MESSAGES) {
      builder
          .minimumExpectedValue(
              profile.getDuration(DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_LOWEST))
          .maximumExpectedValue(
              profile.getDuration(DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_HIGHEST))
          .serviceLevelObjectives(
              profile.isDefined(DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_SLO)
                  ? profile
                      .getDurationList(DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_SLO)
                      .toArray(new Duration[0])
                  : null)
          .percentilePrecision(profile.getInt(DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_DIGITS));
    }
    return builder;
  }
}
