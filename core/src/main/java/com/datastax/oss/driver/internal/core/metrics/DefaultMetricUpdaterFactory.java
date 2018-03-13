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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMetricUpdaterFactory implements MetricUpdaterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricUpdaterFactory.class);

  private final String logPrefix;
  private final InternalDriverContext context;
  private final Set<NodeMetric> enabledNodeMetrics;
  private final SessionMetricUpdater sessionMetricUpdater;

  public DefaultMetricUpdaterFactory(InternalDriverContext context) {
    this.logPrefix = context.sessionName();
    this.context = context;
    DriverConfigProfile config = context.config().getDefaultProfile();
    Set<SessionMetric> enabledSessionMetrics =
        parseSessionMetricPaths(config.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED));
    this.sessionMetricUpdater = new DefaultSessionMetricUpdater(enabledSessionMetrics, context);
    this.enabledNodeMetrics =
        parseNodeMetricPaths(config.getStringList(DefaultDriverOption.METRICS_NODE_ENABLED));
  }

  @Override
  public SessionMetricUpdater getSessionUpdater() {
    return sessionMetricUpdater;
  }

  @Override
  public NodeMetricUpdater newNodeUpdater(Node node) {
    return new DefaultNodeMetricUpdater(node, enabledNodeMetrics, context);
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
