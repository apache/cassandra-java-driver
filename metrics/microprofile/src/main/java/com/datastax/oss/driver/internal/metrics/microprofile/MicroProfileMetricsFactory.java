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
import io.netty.util.concurrent.EventExecutor;
import java.util.Optional;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class MicroProfileMetricsFactory implements MetricsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(MicroProfileMetricsFactory.class);

  private final InternalDriverContext context;
  private final Set<NodeMetric> enabledNodeMetrics;
  private final MetricRegistry registry;
  private final SessionMetricUpdater sessionUpdater;

  public MicroProfileMetricsFactory(DriverContext context) {
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
      LOG.debug("[{}] All metrics are disabled.", logPrefix);
      this.registry = null;
      this.sessionUpdater = NoopSessionMetricUpdater.INSTANCE;
    } else {
      Object possibleMetricRegistry = this.context.getMetricRegistry();
      if (possibleMetricRegistry == null) {
        // metrics are enabled, but a metric registry was not supplied to the context
        throw new IllegalArgumentException(
            "No metric registry object found. Expected registry object to be of type '"
                + MetricRegistry.class.getName()
                + "'");
      }
      if (possibleMetricRegistry instanceof MetricRegistry) {
        this.registry = (MetricRegistry) possibleMetricRegistry;
        this.sessionUpdater =
            new MicroProfileSessionMetricUpdater(
                this.context, enabledSessionMetrics, this.registry);
      } else {
        // Metrics are enabled, but the registry object is not an expected type
        throw new IllegalArgumentException(
            "Unexpected Metrics registry object. Expected registry object to be of type '"
                + MetricRegistry.class.getName()
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
      return new MicroProfileNodeMetricUpdater(node, context, enabledNodeMetrics, registry);
    }
  }

  protected void processNodeStateEvent(NodeStateEvent event) {
    if (event.newState == NodeState.DOWN
        || event.newState == NodeState.FORCED_DOWN
        || event.newState == null) {
      // node is DOWN or REMOVED
      ((MicroProfileNodeMetricUpdater) event.node.getMetricUpdater())
          .startMetricsExpirationTimeout();
    } else if (event.newState == NodeState.UP || event.newState == NodeState.UNKNOWN) {
      // node is UP or ADDED
      ((MicroProfileNodeMetricUpdater) event.node.getMetricUpdater())
          .cancelMetricsExpirationTimeout();
    }
  }
}
