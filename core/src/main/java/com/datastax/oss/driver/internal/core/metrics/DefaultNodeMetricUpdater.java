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

import com.codahale.metrics.Gauge;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.function.Function;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DefaultNodeMetricUpdater extends MetricUpdaterBase<NodeMetric>
    implements NodeMetricUpdater {

  private final String metricNamePrefix;

  public DefaultNodeMetricUpdater(
      Node node, Set<NodeMetric> enabledMetrics, InternalDriverContext context) {
    super(enabledMetrics, context.metricRegistry());
    this.metricNamePrefix = buildPrefix(context.sessionName(), node.getConnectAddress());

    DriverConfigProfile config = context.config().getDefaultProfile();

    if (enabledMetrics.contains(DefaultNodeMetric.OPEN_CONNECTIONS)) {
      metricRegistry.register(
          buildFullName(DefaultNodeMetric.OPEN_CONNECTIONS),
          (Gauge<Integer>) node::getOpenConnections);
    }
    initializePoolGauge(
        DefaultNodeMetric.AVAILABLE_STREAMS, node, ChannelPool::getAvailableIds, context);
    initializePoolGauge(DefaultNodeMetric.IN_FLIGHT, node, ChannelPool::getInFlight, context);
    initializePoolGauge(
        DefaultNodeMetric.ORPHANED_STREAMS, node, ChannelPool::getOrphanedIds, context);
    initializeHdrTimer(
        DefaultNodeMetric.CQL_MESSAGES,
        config,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_INTERVAL);
    initializeDefaultCounter(DefaultNodeMetric.UNSENT_REQUESTS);
    initializeDefaultCounter(DefaultNodeMetric.ABORTED_REQUESTS);
    initializeDefaultCounter(DefaultNodeMetric.WRITE_TIMEOUTS);
    initializeDefaultCounter(DefaultNodeMetric.READ_TIMEOUTS);
    initializeDefaultCounter(DefaultNodeMetric.UNAVAILABLES);
    initializeDefaultCounter(DefaultNodeMetric.OTHER_ERRORS);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES_ON_ABORTED);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES_ON_READ_TIMEOUT);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES_ON_WRITE_TIMEOUT);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES_ON_UNAVAILABLE);
    initializeDefaultCounter(DefaultNodeMetric.RETRIES_ON_OTHER_ERROR);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES_ON_ABORTED);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES_ON_READ_TIMEOUT);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES_ON_WRITE_TIMEOUT);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES_ON_UNAVAILABLE);
    initializeDefaultCounter(DefaultNodeMetric.IGNORES_ON_OTHER_ERROR);
    initializeDefaultCounter(DefaultNodeMetric.SPECULATIVE_EXECUTIONS);
    initializeDefaultCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS);
    initializeDefaultCounter(DefaultNodeMetric.AUTHENTICATION_ERRORS);
  }

  @Override
  protected String buildFullName(NodeMetric metric) {
    return metricNamePrefix + metric.getPath();
  }

  private String buildPrefix(String sessionName, InetSocketAddress addressAndPort) {
    StringBuilder prefix = new StringBuilder(sessionName).append(".nodes.");
    InetAddress address = addressAndPort.getAddress();
    int port = addressAndPort.getPort();
    if (address instanceof Inet4Address) {
      // Metrics use '.' as a delimiter, replace so that the IP is a single path component
      // (127.0.0.1 => 127_0_0_1)
      prefix.append(address.getHostAddress().replace('.', '_'));
    } else {
      assert address instanceof Inet6Address;
      // IPv6 only uses '%' and ':' as separators, so no replacement needed
      prefix.append(address.getHostAddress());
    }
    // Append the port in anticipation of when C* will support nodes on different ports
    return prefix.append('_').append(port).append('.').toString();
  }

  private void initializePoolGauge(
      NodeMetric metric,
      Node node,
      Function<ChannelPool, Integer> reading,
      InternalDriverContext context) {
    if (enabledMetrics.contains(metric)) {
      metricRegistry.register(
          buildFullName(metric),
          (Gauge<Integer>)
              () -> {
                ChannelPool pool = context.poolManager().getPools().get(node);
                return (pool == null) ? 0 : reading.apply(pool);
              });
    }
  }
}
