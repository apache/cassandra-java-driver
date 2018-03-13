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
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import com.datastax.oss.driver.internal.core.session.throttling.RateLimitingRequestThrottler;
import com.datastax.oss.driver.internal.core.session.throttling.RequestThrottler;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSessionMetricUpdater extends MetricUpdaterBase<SessionMetric>
    implements SessionMetricUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSessionMetricUpdater.class);

  private final String metricNamePrefix;

  public DefaultSessionMetricUpdater(
      Set<SessionMetric> enabledMetrics, InternalDriverContext context) {
    super(enabledMetrics, context.metricRegistry());
    this.metricNamePrefix = context.sessionName() + ".";

    if (enabledMetrics.contains(DefaultSessionMetric.CONNECTED_NODES)) {
      metricRegistry.register(
          buildFullName(DefaultSessionMetric.CONNECTED_NODES),
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
    if (enabledMetrics.contains(DefaultSessionMetric.THROTTLING_QUEUE_SIZE)) {
      metricRegistry.register(
          buildFullName(DefaultSessionMetric.THROTTLING_QUEUE_SIZE),
          buildQueueGauge(context.requestThrottler(), context.sessionName()));
    }
    initializeHdrTimer(
        DefaultSessionMetric.CQL_REQUESTS,
        context.config().getDefaultProfile(),
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL);
    initializeDefaultCounter(DefaultSessionMetric.CQL_CLIENT_TIMEOUTS);
    initializeHdrTimer(
        DefaultSessionMetric.THROTTLING_DELAY,
        context.config().getDefaultProfile(),
        DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_INTERVAL);
    initializeDefaultCounter(DefaultSessionMetric.THROTTLING_ERRORS);
  }

  @Override
  protected String buildFullName(SessionMetric metric) {
    return metricNamePrefix + metric.getPath();
  }

  private Gauge<Integer> buildQueueGauge(RequestThrottler requestThrottler, String logPrefix) {
    if (requestThrottler instanceof ConcurrencyLimitingRequestThrottler) {
      return ((ConcurrencyLimitingRequestThrottler) requestThrottler)::getQueueSize;
    } else if (requestThrottler instanceof RateLimitingRequestThrottler) {
      return ((RateLimitingRequestThrottler) requestThrottler)::getQueueSize;
    } else {
      LOG.warn(
          "[{}] Metric {} does not support {}, it will always return 0",
          logPrefix,
          DefaultSessionMetric.THROTTLING_QUEUE_SIZE.getPath(),
          requestThrottler.getClass().getName());
      return () -> 0;
    }
  }
}
