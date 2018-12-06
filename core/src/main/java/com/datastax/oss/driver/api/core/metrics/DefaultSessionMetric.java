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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/** See {@code reference.conf} for a description of each metric. */
public enum DefaultSessionMetric implements SessionMetric {
  BYTES_SENT("bytes-sent"),
  BYTES_RECEIVED("bytes-received"),
  CONNECTED_NODES("connected-nodes"),
  CQL_REQUESTS("cql-requests"),
  CQL_CLIENT_TIMEOUTS("cql-client-timeouts"),
  THROTTLING_DELAY("throttling.delay"),
  THROTTLING_QUEUE_SIZE("throttling.queue-size"),
  THROTTLING_ERRORS("throttling.errors"),
  CQL_PREPARED_CACHE_SIZE("cql-prepared-cache-size"),
  ;

  private static final Map<String, DefaultSessionMetric> BY_PATH = sortByPath();

  private final String path;

  DefaultSessionMetric(String path) {
    this.path = path;
  }

  @NonNull
  @Override
  public String getPath() {
    return path;
  }

  @NonNull
  public static DefaultSessionMetric fromPath(@NonNull String path) {
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
