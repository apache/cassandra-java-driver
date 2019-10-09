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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;

/**
 * A dedicated statement implementation for implicit traversal execution via a {@link
 * DseGraphRemoteConnection}.
 *
 * <p>This is a simplified version of {@link FluentGraphStatement} that exposes the bytecode
 * directly instead of the traversal.
 *
 * <p>This class is for internal use only.
 */
public class BytecodeGraphStatement extends GraphStatementBase<BytecodeGraphStatement> {

  private final Bytecode bytecode;

  public BytecodeGraphStatement(
      Bytecode bytecode, DriverExecutionProfile executionProfile, String executionProfileName) {
    this(
        bytecode,
        null,
        null,
        null,
        Long.MIN_VALUE,
        executionProfile,
        executionProfileName,
        Collections.emptyMap(),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private BytecodeGraphStatement(
      Bytecode bytecode,
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
    this.bytecode = bytecode;
  }

  public Bytecode getBytecode() {
    return bytecode;
  }

  @Override
  protected BytecodeGraphStatement newInstance(
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
    return new BytecodeGraphStatement(
        bytecode,
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
}
