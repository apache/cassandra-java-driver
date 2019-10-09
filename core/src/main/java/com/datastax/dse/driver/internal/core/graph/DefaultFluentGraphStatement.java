/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import net.jcip.annotations.Immutable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

@Immutable
public class DefaultFluentGraphStatement extends GraphStatementBase<FluentGraphStatement>
    implements FluentGraphStatement {

  private final GraphTraversal<?, ?> traversal;

  public DefaultFluentGraphStatement(
      GraphTraversal<?, ?> traversal,
      Boolean isIdempotent,
      Duration timeout,
      Node node,
      long timestamp,
      DriverExecutionProfile executionProfile,
      String executionProfileName,
      Map<String, ByteBuffer> customPayload,
      String graphName,
      String traversalSource,
      String subProtocol,
      ConsistencyLevel consistencyLevel,
      ConsistencyLevel readConsistencyLevel,
      ConsistencyLevel writeConsistencyLevel) {
    super(
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        executionProfileName,
        customPayload,
        graphName,
        traversalSource,
        subProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
    this.traversal = traversal;
  }

  @Override
  protected FluentGraphStatement newInstance(
      Boolean isIdempotent,
      Duration timeout,
      Node node,
      long timestamp,
      DriverExecutionProfile executionProfile,
      String executionProfileName,
      Map<String, ByteBuffer> customPayload,
      String graphName,
      String traversalSource,
      String subProtocol,
      ConsistencyLevel consistencyLevel,
      ConsistencyLevel readConsistencyLevel,
      ConsistencyLevel writeConsistencyLevel) {
    return new DefaultFluentGraphStatement(
        traversal,
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        executionProfileName,
        customPayload,
        graphName,
        traversalSource,
        subProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
  }

  @NonNull
  @Override
  public GraphTraversal<?, ?> getTraversal() {
    return traversal;
  }
}
