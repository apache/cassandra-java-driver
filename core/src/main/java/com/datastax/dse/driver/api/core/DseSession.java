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
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.DefaultMavenCoordinates;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A custom session with DSE-specific capabilities. */
public interface DseSession
    extends CqlSession,
        ContinuousSession,
        GraphSession,
        ReactiveSession,
        ContinuousReactiveSession {

  /**
   * The Maven coordinates of the core DSE driver artifact.
   *
   * <p>Note that this DSE driver depends on the DataStax Java driver for Apache Cassandra&reg;. You
   * can find the coordinates of the Cassandra driver at {@link Session#OSS_DRIVER_COORDINATES}.
   */
  @NonNull
  MavenCoordinates DSE_DRIVER_COORDINATES =
      DefaultMavenCoordinates.buildFromResourceAndPrint(
          DseSession.class.getResource("/com/datastax/dse/driver/Driver.properties"));

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
