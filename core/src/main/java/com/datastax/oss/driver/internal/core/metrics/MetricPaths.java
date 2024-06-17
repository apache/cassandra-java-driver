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

import com.datastax.dse.driver.api.core.metrics.DseNodeMetric;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricPaths {

  private static final Logger LOG = LoggerFactory.getLogger(MetricPaths.class);

  public static Set<SessionMetric> parseSessionMetricPaths(List<String> paths, String logPrefix) {
    Set<SessionMetric> result = new HashSet<>();
    for (String path : paths) {
      try {
        result.add(DefaultSessionMetric.fromPath(path));
      } catch (IllegalArgumentException e) {
        try {
          result.add(DseSessionMetric.fromPath(path));
        } catch (IllegalArgumentException e1) {
          LOG.warn("[{}] Unknown session metric {}, skipping", logPrefix, path);
        }
      }
    }
    return Collections.unmodifiableSet(result);
  }

  public static Set<NodeMetric> parseNodeMetricPaths(List<String> paths, String logPrefix) {
    Set<NodeMetric> result = new HashSet<>();
    for (String path : paths) {
      try {
        result.add(DefaultNodeMetric.fromPath(path));
      } catch (IllegalArgumentException e) {
        try {
          result.add(DseNodeMetric.fromPath(path));
        } catch (IllegalArgumentException e1) {
          LOG.warn("[{}] Unknown node metric {}, skipping", logPrefix, path);
        }
      }
    }
    return Collections.unmodifiableSet(result);
  }
}
