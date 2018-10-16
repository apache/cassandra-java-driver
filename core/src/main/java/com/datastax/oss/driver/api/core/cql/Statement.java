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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.RoutingKey;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * A request to execute a CQL query.
 *
 * @param <SelfT> the "self type" used for covariant returns in subtypes.
 */
public interface Statement<SelfT extends Statement<SelfT>> extends Request {
  // Implementation note: "CqlRequest" would be a better name, but we keep "Statement" to match
  // previous driver versions.

  /**
   * The type returned when a CQL statement is executed synchronously.
   *
   * <p>Most users won't use this explicitly. It is needed for the generic execute method ({@link
   * Session#execute(Request, GenericType)}), but CQL statements will generally be run with one of
   * the driver's built-in helper methods (such as {@link CqlSession#execute(Statement)}).
   */
  GenericType<ResultSet> SYNC = GenericType.of(ResultSet.class);

  /**
   * The type returned when a CQL statement is executed asynchronously.
   *
   * <p>Most users won't use this explicitly. It is needed for the generic execute method ({@link
   * Session#execute(Request, GenericType)}), but CQL statements will generally be run with one of
   * the driver's built-in helper methods (such as {@link CqlSession#executeAsync(Statement)}).
   */
  GenericType<CompletionStage<AsyncResultSet>> ASYNC =
      new GenericType<CompletionStage<AsyncResultSet>>() {};

  /**
   * Sets the name of the execution profile that will be used for this statement.
   *
   * <p>For all the driver's built-in implementations, this method has no effect if {@link
   * #setExecutionProfile(DriverExecutionProfile)} has been called with a non-null argument.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  SelfT setExecutionProfileName(@Nullable String newConfigProfileName);

  /**
   * Sets the execution profile to use for this statement.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  SelfT setExecutionProfile(@Nullable DriverExecutionProfile newProfile);

  /**
   * Sets the keyspace to use for token-aware routing.
   *
   * <p>See {@link Request#getRoutingKey()} for a description of the token-aware routing algorithm.
   *
   * @param newRoutingKeyspace The keyspace to use, or {@code null} to disable token-aware routing.
   */
  @NonNull
  SelfT setRoutingKeyspace(@Nullable CqlIdentifier newRoutingKeyspace);

  /**
   * Sets the {@link Node} that should handle this query.
   *
   * <p>In the general case, use of this method is <em>heavily discouraged</em> and should only be
   * used in the following cases:
   *
   * <ol>
   *   <li>Querying node-local tables, such as tables in the {@code system} and {@code system_views}
   *       keyspaces.
   *   <li>Applying a series of schema changes, where it may be advantageous to execute schema
   *       changes in sequence on the same node.
   * </ol>
   *
   * <p>Configuring a specific node causes the configured {@link LoadBalancingPolicy} to be
   * completely bypassed. However, if the load balancing policy dictates that the node is at
   * distance {@link NodeDistance#IGNORED} or there is no active connectivity to the node, the
   * request will fail with a {@link NoNodeAvailableException}.
   *
   * @param node The node that should be used to handle executions of this statement or null to
   *     delegate to the configured load balancing policy.
   */
  @NonNull
  SelfT setNode(@Nullable Node node);

  /**
   * Shortcut for {@link #setRoutingKeyspace(CqlIdentifier)
   * setRoutingKeyspace(CqlIdentifier.fromCql(newRoutingKeyspaceName))}.
   *
   * @param newRoutingKeyspaceName The keyspace to use, or {@code null} to disable token-aware
   *     routing.
   */
  @NonNull
  default SelfT setRoutingKeyspace(@Nullable String newRoutingKeyspaceName) {
    return setRoutingKeyspace(
        newRoutingKeyspaceName == null ? null : CqlIdentifier.fromCql(newRoutingKeyspaceName));
  }

  /**
   * Sets the key to use for token-aware routing.
   *
   * <p>See {@link Request#getRoutingKey()} for a description of the token-aware routing algorithm.
   *
   * @param newRoutingKey The routing key to use, or {@code null} to disable token-aware routing.
   */
  @NonNull
  SelfT setRoutingKey(@Nullable ByteBuffer newRoutingKey);

  /**
   * Sets the key to use for token-aware routing, when the partition key has multiple components.
   *
   * <p>This method assembles the components into a single byte buffer and passes it to {@link
   * #setRoutingKey(ByteBuffer)}. Neither the individual components, nor the vararg array itself,
   * can be {@code null}.
   */
  @NonNull
  default SelfT setRoutingKey(@NonNull ByteBuffer... newRoutingKeyComponents) {
    return setRoutingKey(RoutingKey.compose(newRoutingKeyComponents));
  }

  /**
   * Sets the token to use for token-aware routing.
   *
   * <p>See {@link Request#getRoutingKey()} for a description of the token-aware routing algorithm.
   *
   * @param newRoutingToken The routing token to use, or {@code null} to disable token-aware
   *     routing.
   */
  @NonNull
  SelfT setRoutingToken(@Nullable Token newRoutingToken);

  /**
   * Sets the custom payload to use for execution.
   *
   * <p>All the driver's built-in statement implementations are immutable, and return a new instance
   * from this method. However custom implementations may choose to be mutable and return the same
   * instance.
   *
   * <p>Note that it's your responsibility to provide a thread-safe map. This can be achieved with a
   * concurrent or immutable implementation, or by making it effectively immutable (meaning that
   * it's never modified after being set on the statement).
   */
  @NonNull
  SelfT setCustomPayload(@NonNull Map<String, ByteBuffer> newCustomPayload);

  /**
   * Sets the idempotence to use for execution.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @param newIdempotence a boolean instance to set a statement-specific value, or {@code null} to
   *     use the default idempotence defined in the configuration.
   */
  @NonNull
  SelfT setIdempotent(@Nullable Boolean newIdempotence);

  /**
   * Sets tracing for execution.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  SelfT setTracing(boolean newTracing);

  /**
   * Returns the query timestamp, in microseconds, to send with the statement.
   *
   * <p>If this is equal to {@link Long#MIN_VALUE}, the {@link TimestampGenerator} configured for
   * this driver instance will be used to generate a timestamp.
   *
   * @see TimestampGenerator
   */
  long getTimestamp();

  /**
   * Sets the query timestamp, in microseconds, to send with the statement.
   *
   * <p>If this is equal to {@link Long#MIN_VALUE}, the {@link TimestampGenerator} configured for
   * this driver instance will be used to generate a timestamp.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see TimestampGenerator
   */
  @NonNull
  SelfT setTimestamp(long newTimestamp);

  /**
   * Sets how long to wait for this request to complete. This is a global limit on the duration of a
   * session.execute() call, including any retries the driver might do.
   *
   * @param newTimeout the timeout to use, or {@code null} to use the default value defined in the
   *     configuration.
   * @see DefaultDriverOption#REQUEST_TIMEOUT
   */
  @NonNull
  SelfT setTimeout(@Nullable Duration newTimeout);

  /**
   * Returns the paging state to send with the statement, or {@code null} if this statement has no
   * paging state.
   *
   * <p>Paging states are used in scenarios where a paged result is interrupted then resumed later.
   * The paging state can only be reused with the exact same statement (same query string, same
   * parameters). It is an opaque value that is only meant to be collected, stored and re-used. If
   * you try to modify its contents or reuse it with a different statement, the results are
   * unpredictable.
   */
  @Nullable
  ByteBuffer getPagingState();

  /**
   * Sets the paging state to send with the statement, or {@code null} if this statement has no
   * paging state.
   *
   * <p>Paging states are used in scenarios where a paged result is interrupted then resumed later.
   * The paging state can only be reused with the exact same statement (same query string, same
   * parameters). It is an opaque value that is only meant to be collected, stored and re-used. If
   * you try to modify its contents or reuse it with a different statement, the results are
   * unpredictable.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance;
   * if you do so, you must override {@link #copy(ByteBuffer)}.
   */
  @NonNull
  SelfT setPagingState(@Nullable ByteBuffer newPagingState);

  /**
   * Returns the page size to use for the statement.
   *
   * @return the set page size, otherwise 0 or a negative value to use the default value defined in
   *     the configuration.
   * @see DefaultDriverOption#REQUEST_PAGE_SIZE
   */
  int getPageSize();

  /**
   * Configures how many rows will be retrieved simultaneously in a single network roundtrip (the
   * goal being to avoid loading too many results in memory at the same time).
   *
   * @param newPageSize the page size to use, set to 0 or a negative value to use the default value
   *     defined in the configuration.
   * @see DefaultDriverOption#REQUEST_PAGE_SIZE
   */
  @NonNull
  SelfT setPageSize(int newPageSize);

  /**
   * Returns the {@link ConsistencyLevel} to use for the statement.
   *
   * @return the set consistency, or {@code null} to use the default value defined in the
   *     configuration.
   * @see DefaultDriverOption#REQUEST_CONSISTENCY
   */
  @Nullable
  ConsistencyLevel getConsistencyLevel();

  /**
   * Sets the {@link ConsistencyLevel} to use for this statement.
   *
   * @param newConsistencyLevel the consistency level to use, or null to use the default value
   *     defined in the configuration.
   * @see DefaultDriverOption#REQUEST_CONSISTENCY
   */
  SelfT setConsistencyLevel(@Nullable ConsistencyLevel newConsistencyLevel);

  /**
   * Returns the serial {@link ConsistencyLevel} to use for the statement.
   *
   * @return the set serial consistency, or {@code null} to use the default value defined in the
   *     configuration.
   * @see DefaultDriverOption#REQUEST_SERIAL_CONSISTENCY
   */
  @Nullable
  ConsistencyLevel getSerialConsistencyLevel();

  /**
   * Sets the serial {@link ConsistencyLevel} to use for this statement.
   *
   * @param newSerialConsistencyLevel the serial consistency level to use, or null to use the
   *     default value defined in the configuration.
   * @see DefaultDriverOption#REQUEST_SERIAL_CONSISTENCY
   */
  @NonNull
  SelfT setSerialConsistencyLevel(@Nullable ConsistencyLevel newSerialConsistencyLevel);

  /** Whether tracing information should be recorded for this statement. */
  boolean isTracing();

  /**
   * Calculates the approximate size in bytes that the statement will have when encoded.
   *
   * <p>The size might be over-estimated by a few bytes due to global options that may be defined on
   * a {@link Session} but not explicitly set on the statement itself.
   *
   * <p>The result of this method is not cached, calling it will cause some encoding to be done in
   * order to determine some of the statement's attributes sizes. Therefore, use this method
   * sparingly in order to avoid unnecessary computation.
   *
   * @return the approximate number of bytes this statement will take when encoded.
   */
  int computeSizeInBytes(@NonNull DriverContext context);

  /**
   * Creates a <b>new instance</b> with a different paging state.
   *
   * <p>Since all the built-in statement implementations in the driver are immutable, this method's
   * default implementation delegates to {@link #setPagingState(ByteBuffer)}. However, if you write
   * your own mutable implementation, make sure it returns a different instance.
   */
  @NonNull
  default SelfT copy(@Nullable ByteBuffer newPagingState) {
    return setPagingState(newPagingState);
  }
}
