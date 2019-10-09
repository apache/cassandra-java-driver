/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
