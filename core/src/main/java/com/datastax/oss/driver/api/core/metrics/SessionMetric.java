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

/**
 * A session-level metric exposed through {@link Session#getMetricRegistry()}.
 *
 * <p>Note that the actual key in the registry is composed of the {@link Session#getName() name of
 * the session} followed by the path of the metric, for example:
 *
 * <pre>
 * // Retrieve the `cql_requests` metric for session `s0`:
 * Timer requestsTimer = session.getMetricRegistry().timer("s0.cql_requests");
 * </pre>
 *
 * <p>All metrics exposed out of the box by the driver are instances of {@link CoreSessionMetric}
 * (this interface only exists to allow custom metrics in driver extensions).
 *
 * @see NodeMetric
 */
public interface SessionMetric {
  String getPath();
}
