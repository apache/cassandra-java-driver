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

import com.datastax.dse.driver.internal.core.graph.DefaultFluentGraphStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/**
 * A graph statement that uses a TinkerPop {@link GraphTraversal} as the query.
 *
 * <p>Typically used like so:
 *
 * <pre>{@code
 * import static com.datastax.dse.driver.api.core.graph.DseGraph.g;
 *
 * FluentGraphStatement statement = FluentGraphStatement.newInstance(g.V().has("name", "marko"));
 *
 * GraphResultSet graphResultSet = dseSession.execute(statement);
 * }</pre>
 *
 * @see DseGraph#g
 */
public interface FluentGraphStatement extends GraphStatement<FluentGraphStatement> {

  /**
   * Create a new instance from the given traversal.
   *
   * <p>Use {@link #builder(GraphTraversal)} if you want to set more options before building the
   * final statement instance.
   */
  @NonNull
  static FluentGraphStatement newInstance(@NonNull GraphTraversal<?, ?> traversal) {
    return new DefaultFluentGraphStatement(
        traversal,
        null,
        null,
        null,
        Statement.NO_DEFAULT_TIMESTAMP,
        null,
        null,
        Collections.emptyMap(),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  /**
   * Create a builder object to start creating a new instance from the given traversal.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static FluentGraphStatementBuilder builder(@NonNull GraphTraversal<?, ?> traversal) {
    return new FluentGraphStatementBuilder(traversal);
  }

  /**
   * Create a builder helper object to start creating a new instance with an existing statement as a
   * template. The traversal and options set on the template will be copied for the new statement at
   * the moment this method is called.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static FluentGraphStatementBuilder builder(@NonNull FluentGraphStatement template) {
    return new FluentGraphStatementBuilder(template);
  }

  /** The underlying TinkerPop object representing the traversal executed by this statement. */
  @NonNull
  GraphTraversal<?, ?> getTraversal();
}
