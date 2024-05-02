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
package com.datastax.dse.driver.api.core.graph;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;

/**
 * A builder helper to create a {@link RemoteConnection} that will be used to build
 * implicitly-executing fluent traversals.
 *
 * <p>To create an instance of this, use the {@link DseGraph#remoteConnectionBuilder(CqlSession)}
 * method:
 *
 * <pre>{@code
 * DseSession dseSession = DseSession.builder().build();
 * GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DseGraph.remoteConnectionBuilder(dseSession).build());
 * List<Vertex> vertices = g.V().hasLabel("person").toList();
 * }</pre>
 *
 * @see CqlSession
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
