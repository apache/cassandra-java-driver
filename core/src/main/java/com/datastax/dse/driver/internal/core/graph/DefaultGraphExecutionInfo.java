/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.GraphExecutionInfo;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.protocol.internal.Frame;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultGraphExecutionInfo implements GraphExecutionInfo {

  private final GraphStatement<?> statement;
  private final Node coordinator;
  private final int speculativeExecutionCount;
  private final int successfulExecutionIndex;
  private final List<Map.Entry<Node, Throwable>> errors;
  private final List<String> warnings;
  private final Map<String, ByteBuffer> customPayload;

  public DefaultGraphExecutionInfo(
      GraphStatement<?> statement,
      Node coordinator,
      int speculativeExecutionCount,
      int successfulExecutionIndex,
      List<Map.Entry<Node, Throwable>> errors,
      Frame frame) {
    this.statement = statement;
    this.coordinator = coordinator;
    this.speculativeExecutionCount = speculativeExecutionCount;
    this.successfulExecutionIndex = successfulExecutionIndex;
    this.errors = errors;

    // Note: the collections returned by the protocol layer are already unmodifiable
    this.warnings = (frame == null) ? Collections.emptyList() : frame.warnings;
    this.customPayload = (frame == null) ? Collections.emptyMap() : frame.customPayload;
  }

  @Override
  public GraphStatement<?> getStatement() {
    return statement;
  }

  @Override
  public Node getCoordinator() {
    return coordinator;
  }

  @Override
  public int getSpeculativeExecutionCount() {
    return speculativeExecutionCount;
  }

  @Override
  public int getSuccessfulExecutionIndex() {
    return successfulExecutionIndex;
  }

  @Override
  public List<Map.Entry<Node, Throwable>> getErrors() {
    // Assume this method will be called 0 or 1 time, so we create the unmodifiable wrapper on
    // demand.
    return (errors == null) ? Collections.emptyList() : Collections.unmodifiableList(errors);
  }

  @Override
  public List<String> getWarnings() {
    return warnings;
  }

  @Override
  public Map<String, ByteBuffer> getIncomingPayload() {
    return customPayload;
  }
}
