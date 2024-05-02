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
package com.datastax.oss.driver.api.core.metrics;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/** See {@code reference.conf} for a description of each metric. */
public enum DefaultNodeMetric implements NodeMetric {
  OPEN_CONNECTIONS("pool.open-connections"),
  AVAILABLE_STREAMS("pool.available-streams"),
  IN_FLIGHT("pool.in-flight"),
  ORPHANED_STREAMS("pool.orphaned-streams"),
  BYTES_SENT("bytes-sent"),
  BYTES_RECEIVED("bytes-received"),
  CQL_MESSAGES("cql-messages"),
  UNSENT_REQUESTS("errors.request.unsent"),
  ABORTED_REQUESTS("errors.request.aborted"),
  WRITE_TIMEOUTS("errors.request.write-timeouts"),
  READ_TIMEOUTS("errors.request.read-timeouts"),
  UNAVAILABLES("errors.request.unavailables"),
  OTHER_ERRORS("errors.request.others"),
  RETRIES("retries.total"),
  RETRIES_ON_ABORTED("retries.aborted"),
  RETRIES_ON_READ_TIMEOUT("retries.read-timeout"),
  RETRIES_ON_WRITE_TIMEOUT("retries.write-timeout"),
  RETRIES_ON_UNAVAILABLE("retries.unavailable"),
  RETRIES_ON_OTHER_ERROR("retries.other"),
  IGNORES("ignores.total"),
  IGNORES_ON_ABORTED("ignores.aborted"),
  IGNORES_ON_READ_TIMEOUT("ignores.read-timeout"),
  IGNORES_ON_WRITE_TIMEOUT("ignores.write-timeout"),
  IGNORES_ON_UNAVAILABLE("ignores.unavailable"),
  IGNORES_ON_OTHER_ERROR("ignores.other"),
  SPECULATIVE_EXECUTIONS("speculative-executions"),
  CONNECTION_INIT_ERRORS("errors.connection.init"),
  AUTHENTICATION_ERRORS("errors.connection.auth"),
  ;

  private static final Map<String, DefaultNodeMetric> BY_PATH = sortByPath();

  private final String path;

  DefaultNodeMetric(String path) {
    this.path = path;
  }

  @Override
  @NonNull
  public String getPath() {
    return path;
  }

  @NonNull
  public static DefaultNodeMetric fromPath(@NonNull String path) {
    DefaultNodeMetric metric = BY_PATH.get(path);
    if (metric == null) {
      throw new IllegalArgumentException("Unknown node metric path " + path);
    }
    return metric;
  }

  private static Map<String, DefaultNodeMetric> sortByPath() {
    ImmutableMap.Builder<String, DefaultNodeMetric> result = ImmutableMap.builder();
    for (DefaultNodeMetric value : values()) {
      result.put(value.getPath(), value);
    }
    return result.build();
  }
}
