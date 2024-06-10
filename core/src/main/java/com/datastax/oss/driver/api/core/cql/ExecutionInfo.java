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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.cql.DefaultPagingState;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Information about the execution of a query.
 *
 * <p>This can be obtained either from a result set for a successful query, or from a driver
 * exception for a failed query.
 *
 * @see ResultSet#getExecutionInfo()
 * @see DriverException#getExecutionInfo()
 */
public interface ExecutionInfo {

  /** @return The {@link Request} that was executed. */
  @NonNull
  default Request getRequest() {
    return getStatement();
  }

  /**
   * @return The {@link Request} that was executed, if it can be cast to {@link Statement}.
   * @deprecated Use {@link #getRequest()} instead.
   * @throws ClassCastException If the request that was executed cannot be cast to {@link
   *     Statement}.
   */
  @NonNull
  @Deprecated
  Statement<?> getStatement();

  /**
   * The node that acted as a coordinator for the query.
   *
   * <p>For successful queries, this is never {@code null}. It is the node that sent the response
   * from which the result was decoded.
   *
   * <p>For failed queries, this can either be {@code null} if the error occurred before any node
   * could be contacted (for example a {@link RequestThrottlingException}), or present if a node was
   * successfully contacted, but replied with an error response (any subclass of {@link
   * CoordinatorException}).
   */
  @Nullable
  Node getCoordinator();

  /**
   * The number of speculative executions that were started for this query.
   *
   * <p>This does not include the initial, normal execution of the query. Therefore, if speculative
   * executions are disabled, this will always be 0. If they are enabled and one speculative
   * execution was triggered in addition to the initial execution, this will be 1, etc.
   *
   * @see SpeculativeExecutionPolicy
   */
  int getSpeculativeExecutionCount();

  /**
   * The index of the execution that completed this query.
   *
   * <p>0 represents the initial, normal execution of the query, 1 the first speculative execution,
   * etc. If this execution info is attached to an error, this might not be applicable, and will
   * return -1.
   *
   * @see SpeculativeExecutionPolicy
   */
  int getSuccessfulExecutionIndex();

  /**
   * The errors encountered on previous coordinators, if any.
   *
   * <p>The list is in chronological order, based on the time that the driver processed the error
   * responses. If speculative executions are enabled, they run concurrently so their errors will be
   * interleaved. A node can appear multiple times (if the retry policy decided to retry on the same
   * node).
   */
  @NonNull
  List<Map.Entry<Node, Throwable>> getErrors();

  /**
   * The paging state of the query, in its raw form.
   *
   * <p>This represents the next page to be fetched if this query has multiple page of results. It
   * can be saved and reused later on the same statement.
   *
   * <p>Note that this is the equivalent of driver 3's {@code getPagingStateUnsafe()}. If you're
   * looking for the method that returns a {@link PagingState}, use {@link #getSafePagingState()}.
   *
   * @return the paging state, or {@code null} if there is no next page.
   */
  @Nullable
  ByteBuffer getPagingState();

  /**
   * The paging state of the query, in a safe wrapper that checks if it's reused on the right
   * statement.
   *
   * <p>This represents the next page to be fetched if this query has multiple page of results. It
   * can be saved and reused later on the same statement.
   *
   * @return the paging state, or {@code null} if there is no next page.
   */
  @Nullable
  default PagingState getSafePagingState() {
    // Default implementation for backward compatibility, but we override it in the concrete class,
    // because it knows the attachment point.
    ByteBuffer rawPagingState = getPagingState();
    if (rawPagingState == null) {
      return null;
    } else {
      Request request = getRequest();
      if (!(request instanceof Statement)) {
        throw new IllegalStateException("Only statements should have a paging state");
      }
      Statement<?> statement = (Statement<?>) request;
      return new DefaultPagingState(rawPagingState, statement, AttachmentPoint.NONE);
    }
  }

  /**
   * The server-side warnings for this query, if any (otherwise the list will be empty).
   *
   * <p>This feature is only available with {@link DefaultProtocolVersion#V4} or above; with lower
   * versions, this list will always be empty.
   */
  @NonNull
  List<String> getWarnings();

  /**
   * The custom payload sent back by the server with the response, if any (otherwise the map will be
   * empty).
   *
   * <p>This method returns a read-only view of the original map, but its values remain inherently
   * mutable. If multiple clients will read these values, care should be taken not to corrupt the
   * data (in particular, preserve the indices by calling {@link ByteBuffer#duplicate()}).
   *
   * <p>This feature is only available with {@link DefaultProtocolVersion#V4} or above; with lower
   * versions, this map will always be empty.
   */
  @NonNull
  Map<String, ByteBuffer> getIncomingPayload();

  /**
   * Whether the cluster reached schema agreement after the execution of this query.
   *
   * <p>After a successful schema-altering query (ex: creating a table), the driver will check if
   * the cluster's nodes agree on the new schema version. If not, it will keep retrying a few times
   * (the retry delay and timeout are set through the configuration).
   *
   * <p>If this method returns {@code false}, clients can call {@link
   * Session#checkSchemaAgreement()} later to perform the check manually.
   *
   * <p>Schema agreement is only checked for schema-altering queries. For other query types, this
   * method will always return {@code true}.
   *
   * @see DefaultDriverOption#CONTROL_CONNECTION_AGREEMENT_INTERVAL
   * @see DefaultDriverOption#CONTROL_CONNECTION_AGREEMENT_TIMEOUT
   */
  boolean isSchemaInAgreement();

  /**
   * The tracing identifier if tracing was {@link Statement#isTracing() enabled} for this query,
   * otherwise {@code null}.
   */
  @Nullable
  UUID getTracingId();

  /**
   * Fetches the query trace asynchronously, if tracing was enabled for this query.
   *
   * <p>Note that each call to this method triggers a new fetch, even if the previous call was
   * successful (this allows fetching the trace again if the list of {@link QueryTrace#getEvents()
   * events} was incomplete).
   *
   * <p>This method will return a failed future if tracing was disabled for the query (that is, if
   * {@link #getTracingId()} is null).
   */
  @NonNull
  CompletionStage<QueryTrace> getQueryTraceAsync();

  /**
   * Convenience method to call {@link #getQueryTraceAsync()} and block for the result.
   *
   * <p>This must not be called on a driver thread.
   *
   * @throws IllegalStateException if {@link #getTracingId()} is null.
   */
  @NonNull
  default QueryTrace getQueryTrace() {
    BlockingOperation.checkNotDriverThread();
    return CompletableFutures.getUninterruptibly(getQueryTraceAsync());
  }

  /**
   * The size of the binary response in bytes.
   *
   * <p>This is the size of the protocol-level frame (including the frame header) before it was
   * decoded by the driver, but after decompression (if compression is enabled).
   *
   * <p>If the information is not available (for example if this execution info comes from an {@link
   * RetryDecision#IGNORE IGNORE} decision of the retry policy), this method returns -1.
   *
   * @see #getCompressedResponseSizeInBytes()
   */
  int getResponseSizeInBytes();

  /**
   * The size of the compressed binary response in bytes.
   *
   * <p>This is the size of the protocol-level frame (including the frame header) as it came in the
   * TCP response, before decompression and decoding by the driver.
   *
   * <p>If compression is disabled, or if the information is not available (for example if this
   * execution info comes from an {@link RetryDecision#IGNORE IGNORE} decision of the retry policy),
   * this method returns -1.
   *
   * @see #getResponseSizeInBytes()
   */
  int getCompressedResponseSizeInBytes();
}
