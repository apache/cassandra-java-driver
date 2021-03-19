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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

/**
 * A {@link MetricIdGenerator} that generates metric identifiers using a combination of names and
 * tags.
 *
 * <p>Session metric identifiers contain a name starting with "session." and ending with the metric
 * path, and a tag with the key "session" and the value of the current session name.
 *
 * <p>Node metric identifiers contain a name starting with "nodes." and ending with the metric path,
 * and two tags: one with the key "session" and the value of the current session name, the other
 * with the key "node" and the value of the current node endpoint.
 */
public class TaggingMetricIdGenerator implements MetricIdGenerator {

  private final String sessionName;
  private final String sessionPrefix;
  private final String nodePrefix;

  @SuppressWarnings("unused")
  public TaggingMetricIdGenerator(DriverContext context) {
    sessionName = context.getSessionName();
    String prefix =
        Objects.requireNonNull(
            context
                .getConfig()
                .getDefaultProfile()
                .getString(DefaultDriverOption.METRICS_ID_GENERATOR_PREFIX, ""));
    sessionPrefix = prefix.isEmpty() ? "session." : prefix + ".session.";
    nodePrefix = prefix.isEmpty() ? "nodes." : prefix + ".nodes.";
  }

  @NonNull
  @Override
  public MetricId sessionMetricId(@NonNull SessionMetric metric) {
    return new DefaultMetricId(
        sessionPrefix + metric.getPath(), ImmutableMap.of("session", sessionName));
  }

  @NonNull
  @Override
  public MetricId nodeMetricId(@NonNull Node node, @NonNull NodeMetric metric) {
    return new DefaultMetricId(
        nodePrefix + metric.getPath(),
        ImmutableMap.of("session", sessionName, "node", node.getEndPoint().toString()));
  }
}
