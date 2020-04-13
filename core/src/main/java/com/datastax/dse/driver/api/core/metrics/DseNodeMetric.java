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
package com.datastax.dse.driver.api.core.metrics;

import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/** See {@code reference.conf} for a description of each metric. */
public enum DseNodeMetric implements NodeMetric {
  GRAPH_MESSAGES("graph-messages");

  private static final Map<String, DseNodeMetric> BY_PATH = sortByPath();

  private final String path;

  DseNodeMetric(String path) {
    this.path = path;
  }

  @Override
  @NonNull
  public String getPath() {
    return path;
  }

  @NonNull
  public static DseNodeMetric fromPath(@NonNull String path) {
    DseNodeMetric metric = BY_PATH.get(path);
    if (metric == null) {
      throw new IllegalArgumentException("Unknown node metric path " + path);
    }
    return metric;
  }

  private static Map<String, DseNodeMetric> sortByPath() {
    ImmutableMap.Builder<String, DseNodeMetric> result = ImmutableMap.builder();
    for (DseNodeMetric value : values()) {
      result.put(value.getPath(), value);
    }
    return result.build();
  }
}
