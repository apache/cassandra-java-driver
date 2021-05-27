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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/**
 * The identifier of a metric.
 *
 * <p>The driver will use the reported name and tags to register the described metric against the
 * current metric registry.
 *
 * <p>A metric identifier is unique, that is, the combination of its name and its tags is expected
 * to be unique for a given metric registry.
 */
public interface MetricId {

  /**
   * Returns this metric name.
   *
   * <p>Metric names can be any non-empty string, but it is recommended to create metric names that
   * have path-like structures separated by a dot, e.g. {@code path.to.my.custom.metric}. Driver
   * built-in implementations of this interface abide by this rule.
   *
   * @return The metric name; cannot be empty nor null.
   */
  @NonNull
  String getName();

  /** @return The metric tags, or empty if no tag is defined; cannot be null. */
  @NonNull
  Map<String, String> getTags();
}
