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
package com.datastax.oss.driver.internal.metrics.microprofile;

import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricId;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.eclipse.microprofile.metrics.MetricRegistry;

@ThreadSafe
public class MicroProfileSessionMetricUpdater extends MicroProfileMetricUpdater<SessionMetric>
    implements SessionMetricUpdater {

  public MicroProfileSessionMetricUpdater(
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

    initializeTimer(DefaultSessionMetric.CQL_REQUESTS, profile);
    initializeTimer(DefaultSessionMetric.THROTTLING_DELAY, profile);
    initializeTimer(DseSessionMetric.CONTINUOUS_CQL_REQUESTS, profile);
    initializeTimer(DseSessionMetric.GRAPH_REQUESTS, profile);
  }

  @Override
  protected MetricId getMetricId(SessionMetric metric) {
    return context.getMetricIdGenerator().sessionMetricId(metric);
  }
}
