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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DefaultMetrics implements Metrics {

  private final MetricRegistry registry;
  private final DropwizardSessionMetricUpdater sessionUpdater;

  public DefaultMetrics(MetricRegistry registry, DropwizardSessionMetricUpdater sessionUpdater) {
    this.registry = registry;
    this.sessionUpdater = sessionUpdater;
  }

  @NonNull
  @Override
  public MetricRegistry getRegistry() {
    return registry;
  }

  @NonNull
  @Override
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T extends Metric> Optional<T> getSessionMetric(
      @NonNull SessionMetric metric, String profileName) {
    return Optional.ofNullable(sessionUpdater.getMetric(metric, profileName));
  }

  @NonNull
  @Override
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T extends Metric> Optional<T> getNodeMetric(
      @NonNull Node node, @NonNull NodeMetric metric, String profileName) {
    NodeMetricUpdater nodeUpdater = ((DefaultNode) node).getMetricUpdater();
    return Optional.ofNullable(
        ((DropwizardNodeMetricUpdater) nodeUpdater).getMetric(metric, profileName));
  }
}
