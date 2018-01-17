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
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.CoreSessionMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.Set;

public class DefaultSessionMetricUpdater extends MetricUpdaterBase<SessionMetric>
    implements SessionMetricUpdater {

  private final String metricNamePrefix;

  public DefaultSessionMetricUpdater(
      Set<SessionMetric> enabledMetrics, InternalDriverContext context) {
    super(enabledMetrics, context.metricRegistry());
    this.metricNamePrefix = context.sessionName() + ".";

    if (enabledMetrics.contains(CoreSessionMetric.CONNECTED_NODES)) {
      metricRegistry.register(
          buildFullName(CoreSessionMetric.CONNECTED_NODES),
          (Gauge<Integer>)
              () -> {
                int count = 0;
                for (Node node : context.metadataManager().getMetadata().getNodes().values()) {
                  if (node.getOpenConnections() > 0) {
                    count += 1;
                  }
                }
                return count;
              });
    }
    initializeHdrTimer(
        CoreSessionMetric.CQL_REQUESTS,
        context.config().getDefaultProfile(),
        CoreDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST,
        CoreDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS,
        CoreDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL);
    initializeDefaultCounter(CoreSessionMetric.CQL_CLIENT_TIMEOUTS);
  }

  @Override
  protected String buildFullName(SessionMetric metric) {
    return metricNamePrefix + metric.getPath();
  }
}
