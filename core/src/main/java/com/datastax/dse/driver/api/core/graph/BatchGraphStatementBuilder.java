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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.NotThreadSafe;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/**
 * A builder to create a batch graph statement.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public class BatchGraphStatementBuilder
    extends GraphStatementBuilderBase<BatchGraphStatementBuilder, BatchGraphStatement> {

  private ImmutableList.Builder<GraphTraversal> traversalsBuilder = ImmutableList.builder();
  private int traversalsCount;

  public BatchGraphStatementBuilder() {
    // nothing to do
  }

  public BatchGraphStatementBuilder(BatchGraphStatement template) {
    super(template);
    traversalsBuilder.addAll(template);
    traversalsCount = template.size();
  }

  /** Add a traversal to this builder to include in the generated {@link BatchGraphStatement}. */
  @NonNull
  public BatchGraphStatementBuilder addTraversal(@NonNull GraphTraversal traversal) {
    traversalsBuilder.add(traversal);
    traversalsCount += 1;
    return this;
  }

  /**
   * Add several traversals to this builder to include in the generated {@link BatchGraphStatement}.
   */
  @NonNull
  public BatchGraphStatementBuilder addTraversals(@NonNull Iterable<GraphTraversal> traversals) {
    for (GraphTraversal traversal : traversals) {
      traversalsBuilder.add(traversal);
      traversalsCount += 1;
    }
    return this;
  }

  /**
   * Add several traversals to this builder to include in the generated {@link BatchGraphStatement}.
   */
  @NonNull
  public BatchGraphStatementBuilder addTraversals(@NonNull GraphTraversal... traversals) {
    for (GraphTraversal traversal : traversals) {
      traversalsBuilder.add(traversal);
      traversalsCount += 1;
    }
    return this;
  }

  /** Clears all the traversals previously added to this builder. */
  @NonNull
  public BatchGraphStatementBuilder clearTraversals() {
    traversalsBuilder = ImmutableList.builder();
    traversalsCount = 0;
    return this;
  }

  /** Returns the number of traversals added to this statement so far. */
  public int getTraversalsCount() {
    return traversalsCount;
  }

  @NonNull
  @Override
  public BatchGraphStatement build() {
    return new DefaultBatchGraphStatement(
        traversalsBuilder.build(),
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        executionProfileName,
        buildCustomPayload(),
        graphName,
        traversalSource,
        subProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
  }
}
