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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Handles conversions from / to GraphExecutionInfo and ExecutionInfo since GraphExecutionInfo has
 * been deprecated by JAVA-2556.
 */
public class GraphExecutionInfoConverter {

  /**
   * Called exclusively from default methods in API interfaces {@link
   * com.datastax.dse.driver.api.core.graph.GraphResultSet} and {@link
   * com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet}. Graph result set implementations
   * do not use this method but rather the other one below.
   */
  @SuppressWarnings("deprecation")
  public static ExecutionInfo convert(
      com.datastax.dse.driver.api.core.graph.GraphExecutionInfo graphExecutionInfo) {
    return new ExecutionInfo() {

      @NonNull
      @Override
      public Request getRequest() {
        return graphExecutionInfo.getStatement();
      }

      @NonNull
      @Override
      public Statement<?> getStatement() {
        throw new ClassCastException("GraphStatement cannot be cast to Statement");
      }

      @Nullable
      @Override
      public Node getCoordinator() {
        return graphExecutionInfo.getCoordinator();
      }

      @Override
      public int getSpeculativeExecutionCount() {
        return graphExecutionInfo.getSpeculativeExecutionCount();
      }

      @Override
      public int getSuccessfulExecutionIndex() {
        return graphExecutionInfo.getSuccessfulExecutionIndex();
      }

      @NonNull
      @Override
      public List<Entry<Node, Throwable>> getErrors() {
        return graphExecutionInfo.getErrors();
      }

      @Nullable
      @Override
      public ByteBuffer getPagingState() {
        return null;
      }

      @NonNull
      @Override
      public List<String> getWarnings() {
        return graphExecutionInfo.getWarnings();
      }

      @NonNull
      @Override
      public Map<String, ByteBuffer> getIncomingPayload() {
        return graphExecutionInfo.getIncomingPayload();
      }

      @Override
      public boolean isSchemaInAgreement() {
        return true;
      }

      @Nullable
      @Override
      public UUID getTracingId() {
        return null;
      }

      @NonNull
      @Override
      public CompletionStage<QueryTrace> getQueryTraceAsync() {
        return CompletableFutures.failedFuture(
            new IllegalStateException("Tracing was disabled for this request"));
      }

      @Override
      public int getResponseSizeInBytes() {
        return -1;
      }

      @Override
      public int getCompressedResponseSizeInBytes() {
        return -1;
      }
    };
  }

  /**
   * Called from graph result set implementations, to convert the original {@link ExecutionInfo}
   * produced by request handlers into the (deprecated) type GraphExecutionInfo.
   */
  @SuppressWarnings("deprecation")
  public static com.datastax.dse.driver.api.core.graph.GraphExecutionInfo convert(
      ExecutionInfo executionInfo) {
    return new com.datastax.dse.driver.api.core.graph.GraphExecutionInfo() {

      @Override
      public GraphStatement<?> getStatement() {
        return (GraphStatement<?>) executionInfo.getRequest();
      }

      @Override
      public Node getCoordinator() {
        return executionInfo.getCoordinator();
      }

      @Override
      public int getSpeculativeExecutionCount() {
        return executionInfo.getSpeculativeExecutionCount();
      }

      @Override
      public int getSuccessfulExecutionIndex() {
        return executionInfo.getSuccessfulExecutionIndex();
      }

      @Override
      public List<Entry<Node, Throwable>> getErrors() {
        return executionInfo.getErrors();
      }

      @Override
      public List<String> getWarnings() {
        return executionInfo.getWarnings();
      }

      @Override
      public Map<String, ByteBuffer> getIncomingPayload() {
        return executionInfo.getIncomingPayload();
      }
    };
  }
}
