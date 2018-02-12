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
package com.datastax.oss.driver.api.core.metrics;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** See {@code reference.conf} for a description of each metric. */
public enum DefaultSessionMetric implements SessionMetric {
  CONNECTED_NODES("connected-nodes"),
  CQL_REQUESTS("cql-requests"),
  CQL_CLIENT_TIMEOUTS("cql-client-timeouts"),
  ;

  private static final Map<String, DefaultSessionMetric> BY_PATH = sortByPath();

  private final String path;

  DefaultSessionMetric(String path) {
    this.path = path;
  }

  @Override
  public String getPath() {
    return path;
  }

  public static DefaultSessionMetric fromPath(String path) {
    DefaultSessionMetric metric = BY_PATH.get(path);
    if (metric == null) {
      throw new IllegalArgumentException("Unknown session metric path " + path);
    }
    return metric;
  }

  private static Map<String, DefaultSessionMetric> sortByPath() {
    ImmutableMap.Builder<String, DefaultSessionMetric> result = ImmutableMap.builder();
    for (DefaultSessionMetric value : values()) {
      result.put(value.getPath(), value);
    }
    return result.build();
  }
}
