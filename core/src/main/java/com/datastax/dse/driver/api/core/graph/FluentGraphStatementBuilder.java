/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import com.datastax.dse.driver.internal.core.graph.DefaultFluentGraphStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.NotThreadSafe;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/**
 * A builder to create a fluent graph statement.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public class FluentGraphStatementBuilder
    extends GraphStatementBuilderBase<FluentGraphStatementBuilder, FluentGraphStatement> {

  private GraphTraversal<?, ?> traversal;

  public FluentGraphStatementBuilder(@NonNull GraphTraversal<?, ?> traversal) {
    this.traversal = traversal;
  }

  public FluentGraphStatementBuilder(@NonNull FluentGraphStatement template) {
    super(template);
    this.traversal = template.getTraversal();
  }

  @NonNull
  @Override
  public FluentGraphStatement build() {
    return new DefaultFluentGraphStatement(
        this.traversal,
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
