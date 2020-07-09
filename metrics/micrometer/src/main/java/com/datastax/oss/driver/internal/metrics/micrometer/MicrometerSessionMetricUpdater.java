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

import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import com.datastax.oss.driver.internal.core.session.throttling.RateLimitingRequestThrottler;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MicrometerSessionMetricUpdater extends MicrometerMetricUpdater<SessionMetric>
    implements SessionMetricUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(MicrometerSessionMetricUpdater.class);

  private final String metricNamePrefix;

  public MicrometerSessionMetricUpdater(
      Set<SessionMetric> enabledMetrics, MeterRegistry registry, DriverContext driverContext) {
    super(enabledMetrics, registry);
    InternalDriverContext context = (InternalDriverContext) driverContext;
    this.metricNamePrefix = context.getSessionName() + ".";

    if (enabledMetrics.contains(DefaultSessionMetric.CONNECTED_NODES)) {
      registerConnectedNodeGauge(context);
    }
    if (enabledMetrics.contains(DefaultSessionMetric.THROTTLING_QUEUE_SIZE)) {
      registerThrottlingQueueGauge(context);
    }
    if (enabledMetrics.contains(DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE)) {
      registerPreparedCacheGauge(context);
    }
    initializeTimer(DefaultSessionMetric.CQL_REQUESTS, context.getConfig().getDefaultProfile());
    initializeDefaultCounter(DefaultSessionMetric.CQL_CLIENT_TIMEOUTS, null);
    initializeTimer(DefaultSessionMetric.THROTTLING_DELAY, context.getConfig().getDefaultProfile());
    initializeDefaultCounter(DefaultSessionMetric.THROTTLING_ERRORS, null);
    initializeTimer(
        DseSessionMetric.CONTINUOUS_CQL_REQUESTS, context.getConfig().getDefaultProfile());
    initializeDefaultCounter(DseSessionMetric.GRAPH_CLIENT_TIMEOUTS, null);
    initializeTimer(DseSessionMetric.GRAPH_REQUESTS, context.getConfig().getDefaultProfile());
  }

  private void registerConnectedNodeGauge(InternalDriverContext context) {
    this.registry.gauge(
        buildFullName(DefaultSessionMetric.CONNECTED_NODES, null),
        context,
        c -> {
          int count = 0;
          for (Node node : c.getMetadataManager().getMetadata().getNodes().values()) {
            if (node.getOpenConnections() > 0) {
              ++count;
            }
          }
          return count;
        });
  }

  private void registerThrottlingQueueGauge(InternalDriverContext context) {
    this.registry.gauge(
        buildFullName(DefaultSessionMetric.THROTTLING_QUEUE_SIZE, null),
        context,
        c -> {
          RequestThrottler requestThrottler = c.getRequestThrottler();
          String logPrefix = c.getSessionName();
          if (requestThrottler instanceof ConcurrencyLimitingRequestThrottler) {
            return ((ConcurrencyLimitingRequestThrottler) requestThrottler).getQueueSize();
          }
          if (requestThrottler instanceof RateLimitingRequestThrottler) {
            return ((RateLimitingRequestThrottler) requestThrottler).getQueueSize();
          }
          LOG.warn(
              "[{}] Metric {} does not support {}, it will always return 0",
              logPrefix,
              DefaultSessionMetric.THROTTLING_QUEUE_SIZE.getPath(),
              requestThrottler.getClass().getName());
          return 0;
        });
  }

  private void registerPreparedCacheGauge(InternalDriverContext context) {
    this.registry.gauge(
        buildFullName(DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE, null),
        context,
        c -> {
          Cache<?, ?> cache = getPreparedStatementCache(c);
          if (cache == null) {
            LOG.warn(
                "[{}] Metric {} is enabled in the config, "
                    + "but it looks like no CQL prepare processor is registered. "
                    + "The gauge will always return 0",
                context.getSessionName(),
                DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE.getPath());
            return 0L;
          }
          return cache.size();
        });
  }

  @Override
  public String buildFullName(SessionMetric metric, String profileName) {
    return metricNamePrefix + metric.getPath();
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
