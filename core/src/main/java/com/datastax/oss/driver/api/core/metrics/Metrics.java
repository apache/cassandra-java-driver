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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.metadata.Node;

/**
 * A wrapper around a {@link MetricRegistry} to expose the driver's metrics.
 *
 * <p>This type exists mainly to avoid a hard dependency to Dropwizard Metrics (that is, the JAR can
 * be completely removed from the classpath if metrics are disabled). It also provides convenience
 * methods to access individual metrics programatically.
 */
public interface Metrics {

  /**
   * Returns the underlying Dropwizard registry.
   *
   * <p>Typically, this can be used to configure a reporter.
   *
   * @see <a href="http://metrics.dropwizard.io/4.0.0/manual/core.html#reporters">Reporters
   *     (Dropwizard Metrics manual)</a>
   */
  MetricRegistry getRegistry();

  /**
   * Retrieves a session-level metric from the registry.
   *
   * <p>To determine the type of each metric, refer to the comments in the default {@code
   * reference.conf} (included in the driver's codebase and JAR file). Note that the method does not
   * check that this type is correct (there is no way to do this at runtime because some metrics are
   * generic); if you use the wrong type, you will get a {@code ClassCastException} in your code:
   *
   * <pre>{@code
   * // Correct:
   * Gauge<Integer> connectedNodes = getNodeMetric(node, DefaultSessionMetric.CONNECTED_NODES);
   *
   * // Wrong, will throw CCE:
   * Counter connectedNodes = getNodeMetric(node, DefaultSessionMetric.CONNECTED_NODES);
   * }</pre>
   *
   * @param profileName the name of the execution profile, or {@code null} if the metric is not
   *     associated to any profile. Note that this is only included for future extensibility: at
   *     this time, the driver does not break up metrics per profile. Therefore you can always use
   *     {@link #getSessionMetric(SessionMetric)} instead of this method.
   * @return the metric, or {@code null} if it is disabled.
   */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  <T extends Metric> T getSessionMetric(SessionMetric metric, String profileName);

  /**
   * Shortcut for {@link #getSessionMetric(SessionMetric, String) getSessionMetric(metric, null)}.
   */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  default <T extends Metric> T getSessionMetric(SessionMetric metric) {
    return getSessionMetric(metric, null);
  }

  /**
   * Retrieves a node-level metric for a given node from the registry.
   *
   * <p>To determine the type of each metric, refer to the comments in the default {@code
   * reference.conf} (included in the driver's codebase and JAR file). Note that the method does not
   * check that this type is correct (there is no way to do this at runtime because some metrics are
   * generic); if you use the wrong type, you will get a {@code ClassCastException} in your code:
   *
   * <pre>{@code
   * // Correct:
   * Gauge<Integer> openConnections = getNodeMetric(node, DefaultNodeMetric.OPEN_CONNECTIONS);
   *
   * // Wrong, will throw CCE:
   * Counter openConnections = getNodeMetric(node, DefaultNodeMetric.OPEN_CONNECTIONS);
   * }</pre>
   *
   * @param profileName the name of the execution profile, or {@code null} if the metric is not
   *     associated to any profile. Note that this is only included for future extensibility: at
   *     this time, the driver does not break up metrics per profile. Therefore you can always use
   *     {@link #getNodeMetric(Node, NodeMetric)} instead of this method.
   * @return the metric, or {@code null} if it is disabled.
   */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  <T extends Metric> T getNodeMetric(Node node, NodeMetric metric, String profileName);

  /**
   * Shortcut for {@link #getNodeMetric(Node, NodeMetric, String) getNodeMetric(node, metric,
   * null)}.
   */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  default <T extends Metric> T getNodeMetric(Node node, NodeMetric metric) {
    return getNodeMetric(node, metric, null);
  }
}
