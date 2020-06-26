/*
 * Copyright DataStax, Inc.
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

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PagingState;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.protocol.internal.Frame;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultExecutionInfo implements ExecutionInfo {

  private final Request request;
  private final Node coordinator;
  private final int speculativeExecutionCount;
  private final int successfulExecutionIndex;
  private final List<Map.Entry<Node, Throwable>> errors;
  private final ByteBuffer pagingState;
  private final UUID tracingId;
  private final int responseSizeInBytes;
  private final int compressedResponseSizeInBytes;
  private final List<String> warnings;
  private final Map<String, ByteBuffer> customPayload;
  private final boolean schemaInAgreement;
  private final DefaultSession session;
  private final InternalDriverContext context;
  private final DriverExecutionProfile executionProfile;

  public DefaultExecutionInfo(
      Request request,
      Node coordinator,
      int speculativeExecutionCount,
      int successfulExecutionIndex,
      List<Map.Entry<Node, Throwable>> errors,
      ByteBuffer pagingState,
      Frame frame,
      boolean schemaInAgreement,
      DefaultSession session,
      InternalDriverContext context,
      DriverExecutionProfile executionProfile) {

    this.request = request;
    this.coordinator = coordinator;
    this.speculativeExecutionCount = speculativeExecutionCount;
    this.successfulExecutionIndex = successfulExecutionIndex;
    this.errors = errors;
    this.pagingState = pagingState;

    this.tracingId = (frame == null) ? null : frame.tracingId;
    this.responseSizeInBytes = (frame == null) ? -1 : frame.size;
    this.compressedResponseSizeInBytes = (frame == null) ? -1 : frame.compressedSize;
    // Note: the collections returned by the protocol layer are already unmodifiable
    this.warnings = (frame == null) ? Collections.emptyList() : frame.warnings;
    this.customPayload = (frame == null) ? Collections.emptyMap() : frame.customPayload;
    this.schemaInAgreement = schemaInAgreement;
    this.session = session;
    this.context = context;
    this.executionProfile = executionProfile;
  }

  @NonNull
  @Override
  @Deprecated
  public Statement<?> getStatement() {
    return (Statement<?>) request;
  }

  @NonNull
  @Override
  public Request getRequest() {
    return request;
  }

  @Nullable
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

  @NonNull
  @Override
  public List<Map.Entry<Node, Throwable>> getErrors() {
    // Assume this method will be called 0 or 1 time, so we create the unmodifiable wrapper on
    // demand.
    return (errors == null) ? Collections.emptyList() : Collections.unmodifiableList(errors);
  }

  @Override
  @Nullable
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @Nullable
  @Override
  public PagingState getSafePagingState() {
    if (pagingState == null) {
      return null;
    } else {
      if (!(request instanceof Statement)) {
        throw new IllegalStateException("Only statements should have a paging state");
      }
      Statement<?> statement = (Statement<?>) request;
      return new DefaultPagingState(pagingState, statement, session.getContext());
    }
  }

  @NonNull
  @Override
  public List<String> getWarnings() {
    return warnings;
  }

  @NonNull
  @Override
  public Map<String, ByteBuffer> getIncomingPayload() {
    return customPayload;
  }

  @Override
  public boolean isSchemaInAgreement() {
    return schemaInAgreement;
  }

  @Override
  @Nullable
  public UUID getTracingId() {
    return tracingId;
  }

  @NonNull
  @Override
  public CompletionStage<QueryTrace> getQueryTraceAsync() {
    if (tracingId == null) {
      return CompletableFutures.failedFuture(
          new IllegalStateException("Tracing was disabled for this request"));
    } else {
      return new QueryTraceFetcher(tracingId, session, context, executionProfile).fetch();
    }
  }

  @Override
  public int getResponseSizeInBytes() {
    return responseSizeInBytes;
  }

  @Override
  public int getCompressedResponseSizeInBytes() {
    return compressedResponseSizeInBytes;
  }
}
