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
import com.datastax.dse.driver.api.core.metrics.DseNodeMetric;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Ticker;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.RemovalNotification;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DropwizardMetricsFactory implements MetricsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DropwizardMetricsFactory.class);
  static final Duration LOWEST_ACCEPTABLE_EXPIRE_AFTER = Duration.ofMinutes(5);
  static final Duration DEFAULT_EXPIRE_AFTER = Duration.ofHours(1);

  private final String logPrefix;
  private final InternalDriverContext context;
  private final Set<NodeMetric> enabledNodeMetrics;
  private final MetricRegistry registry;
  @Nullable private final Metrics metrics;
  private final SessionMetricUpdater sessionUpdater;
  private final Cache<Node, DropwizardNodeMetricUpdater> metricsCache;

  public DropwizardMetricsFactory(InternalDriverContext context, Ticker ticker) {
    this.logPrefix = context.getSessionName();
    this.context = context;

    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    Set<SessionMetric> enabledSessionMetrics =
        parseSessionMetricPaths(config.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED));
    Duration evictionTime = getAndValidateEvictionTime(config, logPrefix);

    this.enabledNodeMetrics =
        parseNodeMetricPaths(config.getStringList(DefaultDriverOption.METRICS_NODE_ENABLED));

    metricsCache =
        CacheBuilder.newBuilder()
            .ticker(ticker)
            .expireAfterAccess(evictionTime)
            .removalListener(
                (RemovalNotification<Node, DropwizardNodeMetricUpdater> notification) -> {
                  LOG.debug(
                      "[{}] Removing metrics for node: {} from cache after {}",
                      logPrefix,
                      notification.getKey(),
                      evictionTime);
                  notification.getValue().cleanupNodeMetrics();
                })
            .build();

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

  @VisibleForTesting
  static Duration getAndValidateEvictionTime(DriverExecutionProfile config, String logPrefix) {
    Duration evictionTime = config.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER);

    if (evictionTime.compareTo(LOWEST_ACCEPTABLE_EXPIRE_AFTER) < 0) {
      LOG.warn(
          "[{}] Value too low for {}: {} (It should be higher than {}). Forcing to {} instead.",
          logPrefix,
          DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER.getPath(),
          evictionTime,
          LOWEST_ACCEPTABLE_EXPIRE_AFTER,
          DEFAULT_EXPIRE_AFTER);
    }

    return evictionTime;
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
    if (registry == null) {
      return NoopNodeMetricUpdater.INSTANCE;
    } else {
      DropwizardNodeMetricUpdater dropwizardNodeMetricUpdater =
          new DropwizardNodeMetricUpdater(
              node, enabledNodeMetrics, registry, context, () -> metricsCache.getIfPresent(node));
      metricsCache.put(node, dropwizardNodeMetricUpdater);
      return dropwizardNodeMetricUpdater;
    }
  }

  protected Set<SessionMetric> parseSessionMetricPaths(List<String> paths) {
    Set<SessionMetric> result = new HashSet<>();
    for (String path : paths) {
      try {
        result.add(DefaultSessionMetric.fromPath(path));
      } catch (IllegalArgumentException e) {
        try {
          result.add(DseSessionMetric.fromPath(path));
        } catch (IllegalArgumentException e1) {
          LOG.warn("[{}] Unknown session metric {}, skipping", logPrefix, path);
        }
      }
    }
    return Collections.unmodifiableSet(result);
  }

  protected Set<NodeMetric> parseNodeMetricPaths(List<String> paths) {
    Set<NodeMetric> result = new HashSet<>();
    for (String path : paths) {
      try {
        result.add(DefaultNodeMetric.fromPath(path));
      } catch (IllegalArgumentException e) {
        try {
          result.add(DseNodeMetric.fromPath(path));
        } catch (IllegalArgumentException e1) {
          LOG.warn("[{}] Unknown node metric {}, skipping", logPrefix, path);
        }
      }
    }
    return Collections.unmodifiableSet(result);
  }
}
