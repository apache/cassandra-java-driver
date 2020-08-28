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
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricPaths;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.NoopSessionMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Ticker;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.RemovalNotification;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class MicroProfileMetricsFactory implements MetricsFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MicroProfileMetricsFactory.class);
  static final Duration LOWEST_ACCEPTABLE_EXPIRE_AFTER = Duration.ofMinutes(5);

  private final InternalDriverContext context;
  private final Set<NodeMetric> enabledNodeMetrics;
  private final MetricRegistry registry;
  private final SessionMetricUpdater sessionUpdater;
  private final Cache<Node, MicroProfileNodeMetricUpdater> metricsCache;

  public MicroProfileMetricsFactory(DriverContext context) {
    this((InternalDriverContext) context, Ticker.systemTicker());
  }

  public MicroProfileMetricsFactory(InternalDriverContext context, Ticker ticker) {
    this.context = context;
    String logPrefix = context.getSessionName();
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    Set<SessionMetric> enabledSessionMetrics =
        MetricPaths.parseSessionMetricPaths(
            config.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED), logPrefix);
    this.enabledNodeMetrics =
        MetricPaths.parseNodeMetricPaths(
            config.getStringList(DefaultDriverOption.METRICS_NODE_ENABLED), logPrefix);

    Duration evictionTime = getAndValidateEvictionTime(config, logPrefix);

    metricsCache =
        CacheBuilder.newBuilder()
            .ticker(ticker)
            .expireAfterAccess(evictionTime)
            .removalListener(
                (RemovalNotification<Node, MicroProfileNodeMetricUpdater> notification) -> {
                  LOG.debug(
                      "[{}] Removing metrics for node: {} from cache after {}",
                      logPrefix,
                      notification.getKey(),
                      evictionTime);
                  notification.getValue().cleanupNodeMetrics();
                })
            .build();

    if (enabledSessionMetrics.isEmpty() && enabledNodeMetrics.isEmpty()) {
      LOG.debug("[{}] All metrics are disabled.", logPrefix);
      this.registry = null;
      this.sessionUpdater = NoopSessionMetricUpdater.INSTANCE;
    } else {
      Object possibleMetricRegistry = context.getMetricRegistry();
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
                enabledSessionMetrics, this.registry, this.context);
      } else {
        // Metrics are enabled, but the registry object is not an expected type
        throw new IllegalArgumentException(
            "Unexpected Metrics registry object. Expected registry object to be of type '"
                + MetricRegistry.class.getName()
                + "', but was '"
                + possibleMetricRegistry.getClass().getName()
                + "'");
      }
    }
  }

  @VisibleForTesting
  static Duration getAndValidateEvictionTime(DriverExecutionProfile config, String logPrefix) {
    Duration evictionTime = config.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER);

    if (evictionTime.compareTo(LOWEST_ACCEPTABLE_EXPIRE_AFTER) < 0) {
      LOG.warn(
          "[{}] Value too low for {}: {}. Forcing to {} instead.",
          logPrefix,
          DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER.getPath(),
          evictionTime,
          LOWEST_ACCEPTABLE_EXPIRE_AFTER);
    }

    return evictionTime;
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
    }
    MicroProfileNodeMetricUpdater updater =
        new MicroProfileNodeMetricUpdater(
            node, enabledNodeMetrics, registry, context, () -> metricsCache.getIfPresent(node));
    metricsCache.put(node, updater);
    return updater;
  }
}
