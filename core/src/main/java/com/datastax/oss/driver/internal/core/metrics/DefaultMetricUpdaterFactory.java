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

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.CoreNodeMetric;
import com.datastax.oss.driver.api.core.metrics.CoreSessionMetric;
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
  private final Set<SessionMetric> enabledSessionMetrics;
  private final Set<NodeMetric> enabledNodeMetrics;

  public DefaultMetricUpdaterFactory(InternalDriverContext context) {
    this.logPrefix = context.sessionName();
    this.context = context;
    DriverConfigProfile config = context.config().getDefaultProfile();
    this.enabledSessionMetrics =
        parseSessionMetricPaths(config.getStringList(CoreDriverOption.METRICS_SESSION_ENABLED));
    this.enabledNodeMetrics =
        parseNodeMetricPaths(config.getStringList(CoreDriverOption.METRICS_NODE_ENABLED));
  }

  @Override
  public SessionMetricUpdater newSessionUpdater() {
    return new DefaultSessionMetricUpdater(enabledSessionMetrics, context);
  }

  @Override
  public NodeMetricUpdater newNodeUpdater(Node node) {
    return new DefaultNodeMetricUpdater(node, enabledNodeMetrics, context);
  }

  private Set<SessionMetric> parseSessionMetricPaths(List<String> paths) {
    EnumSet<CoreSessionMetric> result = EnumSet.noneOf(CoreSessionMetric.class);
    for (String path : paths) {
      try {
        result.add(CoreSessionMetric.fromPath(path));
      } catch (IllegalArgumentException e) {
        LOG.warn("[{}] Unknown session metric {}, skipping", logPrefix, path);
      }
    }
    return Collections.unmodifiableSet(result);
  }

  private Set<NodeMetric> parseNodeMetricPaths(List<String> paths) {
    EnumSet<CoreNodeMetric> result = EnumSet.noneOf(CoreNodeMetric.class);
    for (String path : paths) {
      try {
        result.add(CoreNodeMetric.fromPath(path));
      } catch (IllegalArgumentException e) {
        LOG.warn("[{}] Unknown node metric {}, skipping", logPrefix, path);
      }
    }
    return Collections.unmodifiableSet(result);
  }
}
