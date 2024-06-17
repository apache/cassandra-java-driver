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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import java.util.List;
import java.util.Optional;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class NoopMetricsFactory implements MetricsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(NoopMetricsFactory.class);

  @SuppressWarnings("unused")
  public NoopMetricsFactory(DriverContext context) {
    String logPrefix = context.getSessionName();
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    List<String> enabledSessionMetrics =
        config.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED);
    List<String> enabledNodeMetrics =
        config.getStringList(DefaultDriverOption.METRICS_NODE_ENABLED);
    if (!enabledSessionMetrics.isEmpty() || !enabledNodeMetrics.isEmpty()) {
      LOG.warn(
          "[{}] Some session-level or node-level metrics were enabled, "
              + "but NoopMetricsFactory is being used: all metrics will be empty",
          logPrefix);
    }
  }

  @Override
  public Optional<Metrics> getMetrics() {
    return Optional.empty();
  }

  @Override
  public SessionMetricUpdater getSessionUpdater() {
    return NoopSessionMetricUpdater.INSTANCE;
  }

  @Override
  public NodeMetricUpdater newNodeUpdater(Node node) {
    return NoopNodeMetricUpdater.INSTANCE;
  }
}
