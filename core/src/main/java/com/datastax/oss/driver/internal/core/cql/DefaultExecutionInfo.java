/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.protocol.internal.Frame;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DefaultExecutionInfo implements ExecutionInfo {

  private final Statement statement;
  private final Node coordinator;
  private final int speculativeExecutionCount;
  private final int successfulExecutionIndex;
  private final List<Map.Entry<Node, Throwable>> errors;
  private final ByteBuffer pagingState;
  private final UUID tracingId;
  private final List<String> warnings;
  private final Map<String, ByteBuffer> customPayload;

  public DefaultExecutionInfo(
      Statement statement,
      Node coordinator,
      int speculativeExecutionCount,
      int successfulExecutionIndex,
      List<Map.Entry<Node, Throwable>> errors,
      ByteBuffer pagingState,
      Frame frame) {
    this.statement = statement;
    this.coordinator = coordinator;
    this.speculativeExecutionCount = speculativeExecutionCount;
    this.successfulExecutionIndex = successfulExecutionIndex;
    this.errors = errors;
    this.pagingState = pagingState;

    this.tracingId = (frame == null) ? null : frame.tracingId;
    // Note: the collections returned by the protocol layer are already unmodifiable
    this.warnings = (frame == null) ? Collections.emptyList() : frame.warnings;
    this.customPayload = (frame == null) ? Collections.emptyMap() : frame.customPayload;
  }

  @Override
  public Statement getStatement() {
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
  public ByteBuffer getPagingState() {
    return pagingState;
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
