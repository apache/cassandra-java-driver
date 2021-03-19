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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metrics.MetricPaths;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.NoopSessionMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.util.concurrent.EventExecutor;
import java.util.Optional;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class MicrometerMetricsFactory implements MetricsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(MicrometerMetricsFactory.class);

  private final InternalDriverContext context;
  private final Set<NodeMetric> enabledNodeMetrics;
  private final MeterRegistry registry;
  private final SessionMetricUpdater sessionUpdater;

  public MicrometerMetricsFactory(DriverContext context) {
    this.context = (InternalDriverContext) context;
    String logPrefix = context.getSessionName();
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    Set<SessionMetric> enabledSessionMetrics =
        MetricPaths.parseSessionMetricPaths(
            config.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED), logPrefix);
    this.enabledNodeMetrics =
        MetricPaths.parseNodeMetricPaths(
            config.getStringList(DefaultDriverOption.METRICS_NODE_ENABLED), logPrefix);
    if (enabledSessionMetrics.isEmpty() && enabledNodeMetrics.isEmpty()) {
      LOG.debug("[{}] All metrics are disabled, Session.getMetrics will be empty", logPrefix);
      this.registry = null;
      this.sessionUpdater = NoopSessionMetricUpdater.INSTANCE;
    } else {
      // try to get the metric registry from the context
      Object possibleMetricRegistry = this.context.getMetricRegistry();
      if (possibleMetricRegistry == null) {
        // metrics are enabled, but a metric registry was not supplied to the context
        // use the global registry
        possibleMetricRegistry = io.micrometer.core.instrument.Metrics.globalRegistry;
      }
      if (possibleMetricRegistry instanceof MeterRegistry) {
        this.registry = (MeterRegistry) possibleMetricRegistry;
        this.sessionUpdater =
            new MicrometerSessionMetricUpdater(this.context, enabledSessionMetrics, this.registry);
      } else {
        // Metrics are enabled, but the registry object is not an expected type
        throw new IllegalArgumentException(
            "Unexpected Metrics registry object. Expected registry object to be of type '"
                + MeterRegistry.class.getName()
                + "', but was '"
                + possibleMetricRegistry.getClass().getName()
                + "'");
      }
      if (!enabledNodeMetrics.isEmpty()) {
        EventExecutor adminEventExecutor =
            this.context.getNettyOptions().adminEventExecutorGroup().next();
        this.context
            .getEventBus()
            .register(
                NodeStateEvent.class,
                RunOrSchedule.on(adminEventExecutor, this::processNodeStateEvent));
      }
    }
  }

  @Override
  public Optional<Metrics> getMetrics() {
    return Optional.empty();
  }

  @Override
  public SessionMetricUpdater getSessionUpdater() {
    return sessionUpdater;
  }

  @Override
  public NodeMetricUpdater newNodeUpdater(Node node) {
    if (registry == null) {
      return NoopNodeMetricUpdater.INSTANCE;
    } else {
      return new MicrometerNodeMetricUpdater(node, context, enabledNodeMetrics, registry);
    }
  }

  protected void processNodeStateEvent(NodeStateEvent event) {
    if (event.newState == NodeState.DOWN
        || event.newState == NodeState.FORCED_DOWN
        || event.newState == null) {
      // node is DOWN or REMOVED
      ((MicrometerNodeMetricUpdater) event.node.getMetricUpdater()).startMetricsExpirationTimeout();
    } else if (event.newState == NodeState.UP || event.newState == NodeState.UNKNOWN) {
      // node is UP or ADDED
      ((MicrometerNodeMetricUpdater) event.node.getMetricUpdater())
          .cancelMetricsExpirationTimeout();
    }
  }
}
