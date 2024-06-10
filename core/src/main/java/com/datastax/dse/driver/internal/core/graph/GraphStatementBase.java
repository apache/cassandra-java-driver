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

import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public abstract class GraphStatementBase<SelfT extends GraphStatement<SelfT>>
    implements GraphStatement<SelfT> {
  private final Boolean isIdempotent;
  private final Duration timeout;
  private final Node node;
  private final long timestamp;
  private final DriverExecutionProfile executionProfile;
  private final String executionProfileName;
  private final Map<String, ByteBuffer> customPayload;
  private final String graphName;
  private final String traversalSource;
  private final String subProtocol;
  private final ConsistencyLevel consistencyLevel;
  private final ConsistencyLevel readConsistencyLevel;
  private final ConsistencyLevel writeConsistencyLevel;

  protected GraphStatementBase(
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
    this.isIdempotent = isIdempotent;
    this.timeout = timeout;
    this.node = node;
    this.timestamp = timestamp;
    this.executionProfile = executionProfile;
    this.executionProfileName = executionProfileName;
    this.customPayload = customPayload;
    this.graphName = graphName;
    this.traversalSource = traversalSource;
    this.subProtocol = subProtocol;
    this.consistencyLevel = consistencyLevel;
    this.readConsistencyLevel = readConsistencyLevel;
    this.writeConsistencyLevel = writeConsistencyLevel;
  }

  protected abstract SelfT newInstance(
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
      ConsistencyLevel writeConsistencyLevel);

  @Override
  public Boolean isIdempotent() {
    return isIdempotent;
  }

  @NonNull
  @Override
  public SelfT setIdempotent(@Nullable Boolean newIdempotence) {
    return newInstance(
        newIdempotence,
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

  @Nullable
  @Override
  public Duration getTimeout() {
    return timeout;
  }

  @NonNull
  @Override
  public SelfT setTimeout(@Nullable Duration newTimeout) {
    return newInstance(
        isIdempotent,
        newTimeout,
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

  @Nullable
  @Override
  public Node getNode() {
    return node;
  }

  @NonNull
  @Override
  public SelfT setNode(@Nullable Node newNode) {
    return newInstance(
        isIdempotent,
        timeout,
        newNode,
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

  @Override
  public long getTimestamp() {
    return this.timestamp;
  }

  @NonNull
  @Override
  public SelfT setTimestamp(long newTimestamp) {
    return newInstance(
        isIdempotent,
        timeout,
        node,
        newTimestamp,
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

  @Nullable
  @Override
  public DriverExecutionProfile getExecutionProfile() {
    return executionProfile;
  }

  @NonNull
  @Override
  public SelfT setExecutionProfile(@Nullable DriverExecutionProfile newExecutionProfile) {
    return newInstance(
        isIdempotent,
        timeout,
        node,
        timestamp,
        newExecutionProfile,
        executionProfileName,
        customPayload,
        graphName,
        traversalSource,
        subProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
  }

  @Nullable
  @Override
  public String getExecutionProfileName() {
    return executionProfileName;
  }

  @NonNull
  @Override
  public SelfT setExecutionProfileName(@Nullable String newExecutionProfileName) {
    return newInstance(
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        newExecutionProfileName,
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
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }

  @NonNull
  @Override
  public SelfT setCustomPayload(@NonNull Map<String, ByteBuffer> newCustomPayload) {
    return newInstance(
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        executionProfileName,
        newCustomPayload,
        graphName,
        traversalSource,
        subProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
  }

  @Nullable
  @Override
  public String getGraphName() {
    return graphName;
  }

  @NonNull
  @Override
  public SelfT setGraphName(@Nullable String newGraphName) {
    return newInstance(
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        executionProfileName,
        customPayload,
        newGraphName,
        traversalSource,
        subProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
  }

  @Nullable
  @Override
  public String getTraversalSource() {
    return traversalSource;
  }

  @NonNull
  @Override
  public SelfT setTraversalSource(@Nullable String newTraversalSource) {
    return newInstance(
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        executionProfileName,
        customPayload,
        graphName,
        newTraversalSource,
        subProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
  }

  @Nullable
  @Override
  public String getSubProtocol() {
    return subProtocol;
  }

  @NonNull
  @Override
  public SelfT setSubProtocol(@Nullable String newSubProtocol) {
    return newInstance(
        isIdempotent,
        timeout,
        node,
        timestamp,
        executionProfile,
        executionProfileName,
        customPayload,
        graphName,
        traversalSource,
        newSubProtocol,
        consistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
  }

  @Nullable
  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  @Override
  public SelfT setConsistencyLevel(@Nullable ConsistencyLevel newConsistencyLevel) {
    return newInstance(
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
        newConsistencyLevel,
        readConsistencyLevel,
        writeConsistencyLevel);
  }

  @Nullable
  @Override
  public ConsistencyLevel getReadConsistencyLevel() {
    return readConsistencyLevel;
  }

  @NonNull
  @Override
  public SelfT setReadConsistencyLevel(@Nullable ConsistencyLevel newReadConsistencyLevel) {
    return newInstance(
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
        newReadConsistencyLevel,
        writeConsistencyLevel);
  }

  @Nullable
  @Override
  public ConsistencyLevel getWriteConsistencyLevel() {
    return writeConsistencyLevel;
  }

  @NonNull
  @Override
  public SelfT setWriteConsistencyLevel(@Nullable ConsistencyLevel newWriteConsistencyLevel) {
    return newInstance(
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
        newWriteConsistencyLevel);
  }
}
