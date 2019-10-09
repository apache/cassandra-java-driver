/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver;

import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/** See {@code dse-reference.conf} for a description of each metric. */
public enum DseSessionMetric implements SessionMetric {
  CONTINUOUS_CQL_REQUESTS("continuous-cql-requests"),
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
