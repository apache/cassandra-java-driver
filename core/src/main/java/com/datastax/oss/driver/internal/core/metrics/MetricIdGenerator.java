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
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link MetricIdGenerator} is used to generate the unique identifiers by which a metric should
 * be registered against the current metrics registry.
 *
 * <p>The driver ships with two implementations of this interface; {@code DefaultMetricIdGenerator}
 * and {@code TaggingMetricIdGenerator}.
 *
 * <p>{@code DefaultMetricIdGenerator} is the default implementation; it generates metric
 * identifiers with unique names and no tags.
 *
 * <p>{@code TaggingMetricIdGenerator} generates metric identifiers whose uniqueness stems from the
 * combination of their names and tags.
 *
 * <p>See the driver's {@code reference.conf} file.
 */
public interface MetricIdGenerator {

  /** Generates a {@link MetricId} for the given {@link SessionMetric}. */
  @NonNull
  MetricId sessionMetricId(@NonNull SessionMetric metric);

  /** Generates a {@link MetricId} for the given {@link Node} and {@link NodeMetric}. */
  @NonNull
  MetricId nodeMetricId(@NonNull Node node, @NonNull NodeMetric metric);
}
