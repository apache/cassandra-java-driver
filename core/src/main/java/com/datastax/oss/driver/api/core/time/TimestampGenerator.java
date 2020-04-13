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
package com.datastax.oss.driver.api.core.time;

import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Generates client-side, microsecond-precision query timestamps.
 *
 * <p>These timestamps are used to order queries server-side, and resolve potential conflicts.
 */
public interface TimestampGenerator extends AutoCloseable {

  /**
   * Returns the next timestamp, in <b>microseconds</b>.
   *
   * <p>The timestamps returned by this method should be monotonic; that is, successive invocations
   * should return strictly increasing results. Note that this might not be possible using the clock
   * alone, if it is not precise enough; alternative strategies might include incrementing the last
   * returned value if the clock tick hasn't changed, and possibly drifting in the future. See the
   * built-in driver implementations for more details.
   *
   * @return the next timestamp, or {@link Statement#NO_DEFAULT_TIMESTAMP} to indicate that the
   *     driver should not send one with the query (and let Cassandra generate a server-side
   *     timestamp).
   */
  long next();
}
