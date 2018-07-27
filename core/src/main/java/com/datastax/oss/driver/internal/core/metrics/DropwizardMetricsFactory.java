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
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DropwizardMetricsFactory implements MetricsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DropwizardMetricsFactory.class);

  private final String logPrefix;
  private final InternalDriverContext context;
  private final Set<NodeMetric> enabledNodeMetrics;
  private final MetricRegistry registry;
  @Nullable private final Metrics metrics;
  private final SessionMetricUpdater sessionUpdater;

  public DropwizardMetricsFactory(InternalDriverContext context) {
    this.logPrefix = context.getSessionName();
    this.context = context;

    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    Set<SessionMetric> enabledSessionMetrics =
        parseSessionMetricPaths(config.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED));
    this.enabledNodeMetrics =
        parseNodeMetricPaths(config.getStringList(DefaultDriverOption.METRICS_NODE_ENABLED));

    if (enabledSessionMetrics.isEmpty() && enabledNodeMetrics.isEmpty()) {
      LOG.debug("[{}] All metrics are disabled, Session.getMetrics will be empty", logPrefix);
      this.registry = null;
      this.sessionUpdater = NoopSessionMetricUpdater.INSTANCE;
      this.metrics = null;
    } else {
      this.registry = new MetricRegistry();
      DropwizardSessionMetricUpdater dropwizardSessionUpdater =
          new DropwizardSessionMetricUpdater(enabledSessionMetrics, registry, context);
      this.sessionUpdater = dropwizardSessionUpdater;
      this.metrics = new DefaultMetrics(registry, dropwizardSessionUpdater);
    }
  }

  @Override
  public Optional<Metrics> getMetrics() {
    return Optional.ofNullable(metrics);
  }

  @Override
  public SessionMetricUpdater getSessionUpdater() {
    return sessionUpdater;
  }

  @Override
  public NodeMetricUpdater newNodeUpdater(Node node) {
    return (registry == null)
        ? NoopNodeMetricUpdater.INSTANCE
        : new DropwizardNodeMetricUpdater(node, enabledNodeMetrics, registry, context);
  }

  private Set<SessionMetric> parseSessionMetricPaths(List<String> paths) {
    EnumSet<DefaultSessionMetric> result = EnumSet.noneOf(DefaultSessionMetric.class);
    for (String path : paths) {
      try {
        result.add(DefaultSessionMetric.fromPath(path));
      } catch (IllegalArgumentException e) {
        LOG.warn("[{}] Unknown session metric {}, skipping", logPrefix, path);
      }
    }
    return Collections.unmodifiableSet(result);
  }

  private Set<NodeMetric> parseNodeMetricPaths(List<String> paths) {
    EnumSet<DefaultNodeMetric> result = EnumSet.noneOf(DefaultNodeMetric.class);
    for (String path : paths) {
      try {
        result.add(DefaultNodeMetric.fromPath(path));
      } catch (IllegalArgumentException e) {
        LOG.warn("[{}] Unknown node metric {}, skipping", logPrefix, path);
      }
    }
    return Collections.unmodifiableSet(result);
  }
}
