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

import com.datastax.dse.driver.internal.core.graph.DefaultBatchGraphStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/**
 * A graph statement that groups multiple <b>mutating</b> traversals together, to be executed in the
 * same transaction.
 *
 * <p>It is reserved for graph mutations, and does not return any result.
 *
 * <p>All the mutations grouped in the batch will either all succeed, or they will all be discarded
 * and return an error.
 *
 * <p>The default implementation returned by the driver is immutable and thread-safe. Each mutation
 * operation returns a copy. If you chain many of those operations, it is recommended to use {@link
 * #builder()} instead for better memory usage.
 *
 * <p>Typically used like so:
 *
 * <pre>{@code
 * import static com.datastax.dse.driver.api.core.graph.DseGraph.g;
 *
 * BatchGraphStatement statement =
 *     BatchGraphStatement.builder()
 *         .addTraversal(
 *                 g.addV("person").property("name", "batch1").property("age", 1))
 *         .addTraversal(
 *                 g.addV("person").property("name", "batch2").property("age", 2))
 *         .build();
 *
 * GraphResultSet graphResultSet = dseSession.execute(statement);
 * }</pre>
 *
 * @see DseGraph#g
 */
public interface BatchGraphStatement
    extends GraphStatement<BatchGraphStatement>, Iterable<GraphTraversal> {

  /**
   * Create a new, empty instance.
   *
   * <p>Traversals can be added with {@link #addTraversal(GraphTraversal)}.
   */
  @NonNull
  static BatchGraphStatement newInstance() {
    return new DefaultBatchGraphStatement(
        ImmutableList.of(),
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

  /** Create a new instance from the given list of traversals. */
  @NonNull
  static BatchGraphStatement newInstance(@NonNull Iterable<GraphTraversal> traversals) {
    return new DefaultBatchGraphStatement(
        traversals,
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

  /** Create a new instance from the given list of traversals. */
  @NonNull
  static BatchGraphStatement newInstance(@NonNull GraphTraversal... traversals) {
    return newInstance(ImmutableList.copyOf(traversals));
  }

  /**
   * Create a builder helper object to start creating a new instance.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static BatchGraphStatementBuilder builder() {
    return new BatchGraphStatementBuilder();
  }

  /**
   * Create a builder helper object to start creating a new instance with an existing statement as a
   * template. The traversals and options set on the template will be copied for the new statement
   * at the moment this method is called.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static BatchGraphStatementBuilder builder(@NonNull BatchGraphStatement template) {
    return new BatchGraphStatementBuilder(template);
  }

  /**
   * Add a traversal to this statement. If many traversals need to be added, use a {@link
   * #builder()}, or the {@link #addTraversals(Iterable)} method instead to avoid intermediary
   * copies.
   */
  @NonNull
  BatchGraphStatement addTraversal(@NonNull GraphTraversal traversal);

  /**
   * Adds several traversals to this statement. If this method is to be called many times, consider
   * using a {@link #builder()} instead to avoid intermediary copies.
   */
  @NonNull
  BatchGraphStatement addTraversals(@NonNull Iterable<GraphTraversal> traversals);

  /** Get the number of traversals already added to this statement. */
  int size();
}
