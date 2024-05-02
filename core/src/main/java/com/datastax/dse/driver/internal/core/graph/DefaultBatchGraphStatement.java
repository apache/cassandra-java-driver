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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.BatchGraphStatement;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.Immutable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

@Immutable
public class DefaultBatchGraphStatement extends GraphStatementBase<BatchGraphStatement>
    implements BatchGraphStatement {

  private final List<GraphTraversal> traversals;

  public DefaultBatchGraphStatement(
      Iterable<GraphTraversal> traversals,
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
    this.traversals = ImmutableList.copyOf(traversals);
  }

  @NonNull
  @Override
  public DefaultBatchGraphStatement addTraversal(@NonNull GraphTraversal newTraversal) {
    return new DefaultBatchGraphStatement(
        ImmutableList.<GraphTraversal>builder().addAll(traversals).add(newTraversal).build(),
        isIdempotent(),
        getTimeout(),
        getNode(),
        getTimestamp(),
        getExecutionProfile(),
        getExecutionProfileName(),
        getCustomPayload(),
        getGraphName(),
        getTraversalSource(),
        getSubProtocol(),
        getConsistencyLevel(),
        getReadConsistencyLevel(),
        getWriteConsistencyLevel());
  }

  @NonNull
  @Override
  public DefaultBatchGraphStatement addTraversals(@NonNull Iterable<GraphTraversal> newTraversals) {
    return new DefaultBatchGraphStatement(
        ImmutableList.<GraphTraversal>builder().addAll(traversals).addAll(newTraversals).build(),
        isIdempotent(),
        getTimeout(),
        getNode(),
        getTimestamp(),
        getExecutionProfile(),
        getExecutionProfileName(),
        getCustomPayload(),
        getGraphName(),
        getTraversalSource(),
        getSubProtocol(),
        getConsistencyLevel(),
        getReadConsistencyLevel(),
        getWriteConsistencyLevel());
  }

  @Override
  public int size() {
    return this.traversals.size();
  }

  @Override
  protected BatchGraphStatement newInstance(
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
    return new DefaultBatchGraphStatement(
        traversals,
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
  public Iterator<GraphTraversal> iterator() {
    return this.traversals.iterator();
  }
}
