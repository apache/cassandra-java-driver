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
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import com.datastax.oss.driver.internal.core.session.throttling.RateLimitingRequestThrottler;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DropwizardSessionMetricUpdater extends DropwizardMetricUpdater<SessionMetric>
    implements SessionMetricUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(DropwizardSessionMetricUpdater.class);

  private final String metricNamePrefix;

  public DropwizardSessionMetricUpdater(
      Set<SessionMetric> enabledMetrics, MetricRegistry registry, InternalDriverContext context) {
    super(enabledMetrics, registry);
    this.metricNamePrefix = context.getSessionName() + ".";

    if (enabledMetrics.contains(DefaultSessionMetric.CONNECTED_NODES)) {
      this.registry.register(
          buildFullName(DefaultSessionMetric.CONNECTED_NODES, null),
          (Gauge<Integer>)
              () -> {
                int count = 0;
                for (Node node : context.getMetadataManager().getMetadata().getNodes().values()) {
                  if (node.getOpenConnections() > 0) {
                    count += 1;
                  }
                }
                return count;
              });
    }
    if (enabledMetrics.contains(DefaultSessionMetric.THROTTLING_QUEUE_SIZE)) {
      this.registry.register(
          buildFullName(DefaultSessionMetric.THROTTLING_QUEUE_SIZE, null),
          buildQueueGauge(context.getRequestThrottler(), context.getSessionName()));
    }
    if (enabledMetrics.contains(DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE)) {
      Cache<?, ?> cache = getPreparedStatementCache(context);
      Gauge<Long> gauge;
      if (cache == null) {
        LOG.warn(
            "[{}] Metric {} is enabled in the config, "
                + "but it looks like no CQL prepare processor is registered. "
                + "The gauge will always return 0",
            context.getSessionName(),
            DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE.getPath());
        gauge = () -> 0L;
      } else {
        gauge = cache::size;
      }
      this.registry.register(
          buildFullName(DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE, null), gauge);
    }
    initializeHdrTimer(
        DefaultSessionMetric.CQL_REQUESTS,
        context.getConfig().getDefaultProfile(),
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL);
    initializeDefaultCounter(DefaultSessionMetric.CQL_CLIENT_TIMEOUTS, null);
    initializeHdrTimer(
        DefaultSessionMetric.THROTTLING_DELAY,
        context.getConfig().getDefaultProfile(),
        DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_INTERVAL);
    initializeDefaultCounter(DefaultSessionMetric.THROTTLING_ERRORS, null);
  }

  @Override
  public String buildFullName(SessionMetric metric, String profileName) {
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

  @Nullable
  private static Cache<?, ?> getPreparedStatementCache(InternalDriverContext context) {
    // By default, both the sync processor and the async one are registered and they share the same
    // cache. But with a custom processor registry, there could be only one of the two present.
    for (RequestProcessor<?, ?> processor : context.getRequestProcessorRegistry().getProcessors()) {
      if (processor instanceof CqlPrepareAsyncProcessor) {
        return ((CqlPrepareAsyncProcessor) processor).getCache();
      } else if (processor instanceof CqlPrepareSyncProcessor) {
        return ((CqlPrepareSyncProcessor) processor).getCache();
      }
    }
    return null;
  }
}
