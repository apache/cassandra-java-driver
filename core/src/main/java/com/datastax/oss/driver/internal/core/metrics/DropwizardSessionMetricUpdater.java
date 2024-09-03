/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metrics;

import com.codahale.metrics.MetricRegistry;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DropwizardSessionMetricUpdater extends DropwizardMetricUpdater<SessionMetric>
    implements SessionMetricUpdater {

  public DropwizardSessionMetricUpdater(
      InternalDriverContext context, Set<SessionMetric> enabledMetrics, MetricRegistry registry) {
    super(context, enabledMetrics, registry);

    DriverExecutionProfile profile = context.getConfig().getDefaultProfile();

    initializeGauge(DefaultSessionMetric.CONNECTED_NODES, profile, this::connectedNodes);
    initializeGauge(DefaultSessionMetric.THROTTLING_QUEUE_SIZE, profile, this::throttlingQueueSize);
    initializeGauge(
        DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE, profile, this::preparedStatementCacheSize);

    initializeCounter(DefaultSessionMetric.CQL_CLIENT_TIMEOUTS, profile);
    initializeCounter(DefaultSessionMetric.THROTTLING_ERRORS, profile);
    initializeCounter(DseSessionMetric.GRAPH_CLIENT_TIMEOUTS, profile);

    initializeHdrTimer(
        DefaultSessionMetric.CQL_REQUESTS,
        profile,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL);
    initializeHdrTimer(
        DefaultSessionMetric.SEND_LATENCY,
        profile,
        DefaultDriverOption.METRICS_SESSION_SEND_LATENCY_HIGHEST,
        DefaultDriverOption.METRICS_SESSION_SEND_LATENCY_DIGITS,
        DefaultDriverOption.METRICS_SESSION_SEND_LATENCY_INTERVAL);
    initializeHdrTimer(
        DefaultSessionMetric.THROTTLING_DELAY,
        profile,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_INTERVAL);
    initializeHdrTimer(
        DseSessionMetric.CONTINUOUS_CQL_REQUESTS,
        profile,
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_HIGHEST,
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_DIGITS,
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_INTERVAL);
    initializeHdrTimer(
        DseSessionMetric.GRAPH_REQUESTS,
        profile,
        DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_HIGHEST,
        DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_DIGITS,
        DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_INTERVAL);
  }

  @Override
  protected MetricId getMetricId(SessionMetric metric) {
    MetricId id = context.getMetricIdGenerator().sessionMetricId(metric);
    if (!id.getTags().isEmpty()) {
      throw new IllegalStateException("Cannot use metric tags with Dropwizard");
    }
    return id;
  }
}
