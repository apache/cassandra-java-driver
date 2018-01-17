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

import com.datastax.oss.driver.api.core.session.Session;
import java.net.InetAddress;

/**
 * A node-level metric exposed through {@link Session#getMetricRegistry()}.
 *
 * <p>Note that the actual key in the registry is composed of the {@link Session#getName() name of
 * the session}, followed by "nodes", followed by a textual representation of the address of the
 * node, followed by the path of the metric. IPv4 addresses are represented as the decimal
 * components followed by the port, separated with underscores; IPv6 addresses are represented as
 * the result of {@link InetAddress#getHostAddress()}, followed by an underscore, followed by the
 * port. For example:
 *
 * <pre>
 * // Retrieve the `retries.total` metric for node 127.0.0.1:9042 in session `s0`:
 * Meter retriesMeter = session.getMetricRegistry().meter("s0.nodes.127_0_0_1_9042.retries.total");
 *
 * // Retrieve the `retries.total` metric for node ::1:9042 in session `s0`:
 * Meter retriesMeter = session.getMetricRegistry().meter("s0.nodes.0:0:0:0:0:0:0:1_9042.retries.total");
 * </pre>
 *
 * <p>All metrics exposed out of the box by the driver are instances of {@link CoreNodeMetric} (this
 * interface only exists to allow custom metrics in driver extensions).
 *
 * @see SessionMetric
 */
public interface NodeMetric {
  String getPath();
}
