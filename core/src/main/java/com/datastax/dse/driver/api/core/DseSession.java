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
package com.datastax.dse.driver.api.core;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousSession;
import com.datastax.dse.driver.api.core.cql.continuous.reactive.ContinuousReactiveSession;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveSession;
import com.datastax.dse.driver.api.core.graph.GraphSession;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MavenCoordinates;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A custom session with DSE-specific capabilities. */
public interface DseSession
    extends CqlSession,
        ContinuousSession,
        GraphSession,
        ReactiveSession,
        ContinuousReactiveSession {

  /**
   * @deprecated the DSE driver is now part of the DataStax Java driver for Apache Cassandra&reg;.
   *     This field is preserved for backward compatibility, but it returns the same value as {@link
   *     CqlSession#OSS_DRIVER_COORDINATES}.
   */
  @Deprecated @NonNull MavenCoordinates DSE_DRIVER_COORDINATES = CqlSession.OSS_DRIVER_COORDINATES;

  /**
   * Returns a builder to create a new instance.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static DseSessionBuilder builder() {
    return new DseSessionBuilder();
  }
}
