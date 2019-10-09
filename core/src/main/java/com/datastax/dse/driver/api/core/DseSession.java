/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
