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

import com.codahale.metrics.MetricRegistry;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.metrics.DseNodeMetric;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DropwizardNodeMetricUpdater extends DropwizardMetricUpdater<NodeMetric>
    implements NodeMetricUpdater {

  private final Node node;

  public DropwizardNodeMetricUpdater(
      Node node,
      InternalDriverContext context,
      Set<NodeMetric> enabledMetrics,
      MetricRegistry registry) {
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

    initializeHdrTimer(
        DefaultNodeMetric.CQL_MESSAGES,
        profile,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_INTERVAL);
    initializeHdrTimer(
        DseNodeMetric.GRAPH_MESSAGES,
        profile,
        DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_HIGHEST,
        DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_DIGITS,
        DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_INTERVAL);
  }

  @Override
  protected MetricId getMetricId(NodeMetric metric) {
    MetricId id = context.getMetricIdGenerator().nodeMetricId(node, metric);
    if (!id.getTags().isEmpty()) {
      throw new IllegalStateException("Cannot use metric tags with Dropwizard");
    }
    return id;
  }
}
