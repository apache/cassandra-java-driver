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

import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/** See {@code reference.conf} for a description of each metric. */
public enum DseSessionMetric implements SessionMetric {
  CONTINUOUS_CQL_REQUESTS("continuous-cql-requests"),
  GRAPH_REQUESTS("graph-requests"),
  GRAPH_CLIENT_TIMEOUTS("graph-client-timeouts"),
  ;

  private static final Map<String, DseSessionMetric> BY_PATH = sortByPath();

  private final String path;

  DseSessionMetric(String path) {
    this.path = path;
  }

  @NonNull
  @Override
  public String getPath() {
    return path;
  }

  @NonNull
  public static DseSessionMetric fromPath(@NonNull String path) {
    DseSessionMetric metric = BY_PATH.get(path);
    if (metric == null) {
      throw new IllegalArgumentException("Unknown DSE session metric path " + path);
    }
    return metric;
  }

  private static Map<String, DseSessionMetric> sortByPath() {
    ImmutableMap.Builder<String, DseSessionMetric> result = ImmutableMap.builder();
    for (DseSessionMetric value : values()) {
      result.put(value.getPath(), value);
    }
    return result.build();
  }
}
