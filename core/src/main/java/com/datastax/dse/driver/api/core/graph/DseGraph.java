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

import com.datastax.dse.driver.internal.core.graph.DefaultDseRemoteConnectionBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

/**
 * General purpose utility class for interaction with DSE Graph via the DataStax Enterprise Java
 * driver.
 */
public class DseGraph {

  /**
   * A general-purpose shortcut for a <b>non-connected</b> TinkerPop {@link GraphTraversalSource}
   * based on an immutable empty graph. This is really just a shortcut to {@code
   * EmptyGraph.instance().traversal();}.
   *
   * <p>Can be used to create {@link FluentGraphStatement} instances (recommended), or can be
   * configured to be remotely connected to DSE Graph using the {@link #remoteConnectionBuilder}
   * method.
   *
   * <p>For ease of use you may statically import this variable.
   *
   * <p>Calling {@code g.getGraph()} will return a local immutable empty graph which is in no way
   * connected to the DSE Graph server, it will not allow to modify a DSE Graph directly. To act on
   * data stored in DSE Graph you must use {@linkplain
   * org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal traversal}s such as
   * {@code DseGraph.g.V()}, {@code DseGraph.g.addV/addE()}.
   */
  public static final GraphTraversalSource g = EmptyGraph.instance().traversal();

  /**
   * Returns a builder helper class to help create {@link
   * org.apache.tinkerpop.gremlin.process.remote.RemoteConnection} implementations that seamlessly
   * connect to DSE Graph using the {@link CqlSession} in parameter.
   */
  public static DseGraphRemoteConnectionBuilder remoteConnectionBuilder(CqlSession dseSession) {
    return new DefaultDseRemoteConnectionBuilder(dseSession);
  }

  private DseGraph() {
    // nothing to do
  }
}
