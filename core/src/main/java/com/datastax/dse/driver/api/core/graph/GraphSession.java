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
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * A session that has the ability to execute DSE Graph requests.
 *
 * <p>Generally this interface won't be referenced directly in an application; instead, you should
 * use {@link CqlSession}, which is a combination of this interface and many others for a more
 * integrated usage of DataStax Enterprise's multi-model database via a single entry point. However,
 * it is still possible to cast a {@code CqlSession} to a {@code GraphSession} to only expose the
 * DSE Graph execution methods.
 */
public interface GraphSession extends Session {

  /**
   * Executes a graph statement synchronously (the calling thread blocks until the result becomes
   * available).
   *
   * <p>The driver provides different kinds of graph statements:
   *
   * <ul>
   *   <li>{@link FluentGraphStatement} (recommended): wraps a fluent TinkerPop {@linkplain
   *       org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal traversal};
   *   <li>{@link BatchGraphStatement}: groups together multiple mutating traversals ({@code
   *       g.addV()/g.addE()}) inside a single transaction and avoids multiple client-server
   *       round-trips. Improves performance in data ingestion scenarios;
   *   <li>{@link ScriptGraphStatement}: wraps a Gremlin-groovy script provided as a plain Java
   *       string. Required for administrative queries such as creating/dropping a graph,
   *       configuration and schema.
   * </ul>
   *
   * <p>This feature is only available with DataStax Enterprise. Executing graph queries against an
   * Apache Cassandra&reg; cluster will result in a runtime error.
   *
   * @see GraphResultSet
   * @param graphStatement the graph query to execute (that can be any {@code GraphStatement}).
   * @return the result of the graph query. That result will never be null but can be empty.
   */
  @NonNull
  default GraphResultSet execute(@NonNull GraphStatement<?> graphStatement) {
    return Objects.requireNonNull(
        execute(graphStatement, GraphStatement.SYNC),
        "The graph processor should never return a null result");
  }

  /**
   * Executes a graph statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   *
   * <p>This feature is only available with DataStax Enterprise. Executing graph queries against an
   * Apache Cassandra&reg; cluster will result in a runtime error.
   *
   * @see #execute(GraphStatement)
   * @see AsyncGraphResultSet
   * @param graphStatement the graph query to execute (that can be any {@code GraphStatement}).
   * @return the {@code CompletionStage} on the result of the graph query.
   */
  @NonNull
  default CompletionStage<AsyncGraphResultSet> executeAsync(
      @NonNull GraphStatement<?> graphStatement) {
    return Objects.requireNonNull(
        execute(graphStatement, GraphStatement.ASYNC),
        "The graph processor should never return a null result");
  }
}
