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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import com.datastax.oss.driver.internal.core.session.throttling.RateLimitingRequestThrottler;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMetricUpdater<MetricT> implements MetricUpdater<MetricT> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetricUpdater.class);

  protected final InternalDriverContext context;
  protected final Set<MetricT> enabledMetrics;

  protected AbstractMetricUpdater(InternalDriverContext context, Set<MetricT> enabledMetrics) {
    this.context = context;
    this.enabledMetrics = enabledMetrics;
  }

  @Override
  public boolean isEnabled(MetricT metric, String profileName) {
    return enabledMetrics.contains(metric);
  }

  protected int connectedNodes() {
    int count = 0;
    for (Node node : context.getMetadataManager().getMetadata().getNodes().values()) {
      if (node.getOpenConnections() > 0) {
        count++;
      }
    }
    return count;
  }

  protected int throttlingQueueSize() {
    RequestThrottler requestThrottler = context.getRequestThrottler();
    String logPrefix = context.getSessionName();
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
  }

  protected long preparedStatementCacheSize() {
    Cache<?, ?> cache = getPreparedStatementCache();
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
  }

  @Nullable
  protected Cache<?, ?> getPreparedStatementCache() {
    // By default, both the sync processor and the async ones are registered and they share the same
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

  protected int availableStreamIds(Node node) {
    ChannelPool pool = context.getPoolManager().getPools().get(node);
    return (pool == null) ? 0 : pool.getAvailableIds();
  }

  protected int inFlightRequests(Node node) {
    ChannelPool pool = context.getPoolManager().getPools().get(node);
    return (pool == null) ? 0 : pool.getInFlight();
  }

  protected int orphanedStreamIds(Node node) {
    ChannelPool pool = context.getPoolManager().getPools().get(node);
    return (pool == null) ? 0 : pool.getOrphanedIds();
  }
}
