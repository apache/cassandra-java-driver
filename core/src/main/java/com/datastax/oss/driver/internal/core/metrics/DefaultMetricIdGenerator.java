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
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/**
 * The default {@link MetricIdGenerator}.
 *
 * <p>This generator generates unique names, containing the session name, the node endpoint (for
 * node metrics), and the metric prefix. It does not generate tags.
 */
public class DefaultMetricIdGenerator implements MetricIdGenerator {

  private final String sessionPrefix;
  private final String nodePrefix;

  @SuppressWarnings("unused")
  public DefaultMetricIdGenerator(DriverContext context) {
    String sessionName = context.getSessionName();
    String prefix =
        Objects.requireNonNull(
            context
                .getConfig()
                .getDefaultProfile()
                .getString(DefaultDriverOption.METRICS_ID_GENERATOR_PREFIX, ""));
    sessionPrefix = prefix.isEmpty() ? sessionName + '.' : prefix + '.' + sessionName + '.';
    nodePrefix = sessionPrefix + "nodes.";
  }

  @NonNull
  @Override
  public MetricId sessionMetricId(@NonNull SessionMetric metric) {
    return new DefaultMetricId(sessionPrefix + metric.getPath(), ImmutableMap.of());
  }

  @NonNull
  @Override
  public MetricId nodeMetricId(@NonNull Node node, @NonNull NodeMetric metric) {
    return new DefaultMetricId(
        nodePrefix + node.getEndPoint().asMetricPrefix() + '.' + metric.getPath(),
        ImmutableMap.of());
  }
}
