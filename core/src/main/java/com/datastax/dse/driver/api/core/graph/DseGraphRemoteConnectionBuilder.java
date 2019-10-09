/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;

/**
 * A builder helper to create a {@link RemoteConnection} that will be used to build
 * implicitly-executing fluent traversals.
 *
 * <p>To create an instance of this, use the {@link DseGraph#remoteConnectionBuilder(DseSession)}
 * method:
 *
 * <pre>{@code
 * DseSession dseSession = DseSession.builder().build();
 * GraphTraversalSource g = DseGraph.g.withRemote(DseGraph.remoteConnectionBuilder(dseSession).build());
 * List<Vertex> vertices = g.V().hasLabel("person").toList();
 * }</pre>
 *
 * @see DseSession
 */
public interface DseGraphRemoteConnectionBuilder {

  /** Build the remote connection that was configured with this builder. */
  RemoteConnection build();

  /**
   * Set a configuration profile that will be used for every traversal built using the remote
   * connection.
   *
   * <p>For the list of options available for Graph requests, see the {@code reference.conf}
   * configuration file.
   */
  DseGraphRemoteConnectionBuilder withExecutionProfile(DriverExecutionProfile executionProfile);

  /**
   * Set the name of an execution profile that will be used for every traversal using from the
   * remote connection. Named profiles are pre-defined in the driver configuration.
   *
   * <p>For the list of options available for Graph requests, see the {@code reference.conf}
   * configuration file.
   */
  DseGraphRemoteConnectionBuilder withExecutionProfileName(String executionProfileName);
}
