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
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import edu.umd.cs.findbugs.annotations.CheckReturnValue;
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
   * A special value for {@link #getQueryTimestamp()} that means "no value".
   *
   * <p>It is equal to {@link Long#MIN_VALUE}.
   */
  long NO_DEFAULT_TIMESTAMP = QueryOptions.NO_DEFAULT_TIMESTAMP;

  /**
   * A special value for {@link #getNowInSeconds()} that means "no value".
   *
   * <p>It is equal to {@link Integer#MIN_VALUE}.
   */
  int NO_NOW_IN_SECONDS = QueryOptions.NO_NOW_IN_SECONDS;

  /**
   * Sets the name of the execution profile that will be used for this statement.
   *
   * <p>For all the driver's built-in implementations, calling this method with a non-null argument
   * automatically resets {@link #getExecutionProfile()} to null.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  @CheckReturnValue
  SelfT setExecutionProfileName(@Nullable String newConfigProfileName);

  /**
   * Sets the execution profile to use for this statement.
   *
   * <p>For all the driver's built-in implementations, calling this method with a non-null argument
   * automatically resets {@link #getExecutionProfileName()} to null.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  @CheckReturnValue
  SelfT setExecutionProfile(@Nullable DriverExecutionProfile newProfile);

  /**
   * Sets the keyspace to use for token-aware routing.
   *
   * <p>See {@link Request#getRoutingKey()} for a description of the token-aware routing algorithm.
   *
   * @param newRoutingKeyspace The keyspace to use, or {@code null} to disable token-aware routing.
   */
  @NonNull
  @CheckReturnValue
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
  @CheckReturnValue
  SelfT setNode(@Nullable Node node);

  /**
   * Shortcut for {@link #setRoutingKeyspace(CqlIdentifier)
   * setRoutingKeyspace(CqlIdentifier.fromCql(newRoutingKeyspaceName))}.
   *
   * @param newRoutingKeyspaceName The keyspace to use, or {@code null} to disable token-aware
   *     routing.
   */
  @NonNull
  @CheckReturnValue
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
  @CheckReturnValue
  SelfT setRoutingKey(@Nullable ByteBuffer newRoutingKey);

  /**
   * Sets the key to use for token-aware routing, when the partition key has multiple components.
   *
   * <p>This method assembles the components into a single byte buffer and passes it to {@link
   * #setRoutingKey(ByteBuffer)}. Neither the individual components, nor the vararg array itself,
   * can be {@code null}.
   */
  @NonNull
  @CheckReturnValue
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
  @CheckReturnValue
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
  @CheckReturnValue
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
  @CheckReturnValue
  SelfT setIdempotent(@Nullable Boolean newIdempotence);

  /**
   * Sets tracing for execution.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  @CheckReturnValue
  SelfT setTracing(boolean newTracing);

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #setTracing(boolean) setTracing(true)}.
   */
  @Deprecated
  @NonNull
  @CheckReturnValue
  default SelfT enableTracing() {
    return setTracing(true);
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #setTracing(boolean) setTracing(false)}.
   */
  @Deprecated
  @NonNull
  @CheckReturnValue
  default SelfT disableTracing() {
    return setTracing(false);
  }

  /**
   * Returns the query timestamp, in microseconds, to send with the statement. See {@link
   * #setQueryTimestamp(long)} for details.
   *
   * <p>If this is equal to {@link #NO_DEFAULT_TIMESTAMP}, the {@link TimestampGenerator} configured
   * for this driver instance will be used to generate a timestamp.
   *
   * @see #NO_DEFAULT_TIMESTAMP
   * @see TimestampGenerator
   */
  long getQueryTimestamp();

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #getQueryTimestamp()}.
   */
  @Deprecated
  default long getDefaultTimestamp() {
    return getQueryTimestamp();
  }

  /**
   * Sets the query timestamp, in microseconds, to send with the statement.
   *
   * <p>This is an alternative to appending a {@code USING TIMESTAMP} clause in the statement's
   * query string, and has the advantage of sending the timestamp separately from the query string
   * itself, which doesn't have to be modified when executing the same statement with different
   * timestamps. Note that, if both a {@code USING TIMESTAMP} clause and a query timestamp are set
   * for a given statement, the timestamp from the {@code USING TIMESTAMP} clause wins.
   *
   * <p>This method can be used on any instance of {@link SimpleStatement}, {@link BoundStatement}
   * or {@link BatchStatement}. For a {@link BatchStatement}, the timestamp will apply to all its
   * child statements; it is not possible to define per-child timestamps using this method, and
   * consequently, if this method is called on a batch child statement, the provided timestamp will
   * be silently ignored. If different timestamps are required for individual child statements, this
   * can only be achieved with a custom {@code USING TIMESTAMP} clause in each child query.
   *
   * <p>If this is equal to {@link #NO_DEFAULT_TIMESTAMP}, the {@link TimestampGenerator} configured
   * for this driver instance will be used to generate a timestamp.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #NO_DEFAULT_TIMESTAMP
   * @see TimestampGenerator
   */
  @NonNull
  @CheckReturnValue
  SelfT setQueryTimestamp(long newTimestamp);

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #setQueryTimestamp(long)}.
   */
  @Deprecated
  @NonNull
  @CheckReturnValue
  default SelfT setDefaultTimestamp(long newTimestamp) {
    return setQueryTimestamp(newTimestamp);
  }

  /**
   * Sets how long to wait for this request to complete. This is a global limit on the duration of a
   * session.execute() call, including any retries the driver might do.
   *
   * @param newTimeout the timeout to use, or {@code null} to use the default value defined in the
   *     configuration.
   * @see DefaultDriverOption#REQUEST_TIMEOUT
   */
  @NonNull
  @CheckReturnValue
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
  @CheckReturnValue
  SelfT setPagingState(@Nullable ByteBuffer newPagingState);

  /**
   * Sets the paging state to send with the statement, or {@code null} if this statement has no
   * paging state.
   *
   * <p>This variant uses the "safe" paging state wrapper, it will throw immediately if the
   * statement doesn't match the one that the state was initially extracted from (same query string,
   * same parameters). The advantage is that it fails fast, instead of waiting for an error response
   * from the server.
   *
   * <p>Note that, if this statement is a {@link SimpleStatement} with bound values, those values
   * must be encoded in order to perform the check. This method uses the default codec registry and
   * default protocol version. This might fail if you use custom codecs; in that case, use {@link
   * #setPagingState(PagingState, Session)} instead.
   *
   * @throws IllegalArgumentException if the given state does not match this statement.
   * @see #setPagingState(ByteBuffer)
   * @see ExecutionInfo#getSafePagingState()
   */
  @NonNull
  @CheckReturnValue
  default SelfT setPagingState(@Nullable PagingState newPagingState) {
    return setPagingState(newPagingState, null);
  }

  /**
   * Alternative to {@link #setPagingState(PagingState)} that specifies the session the statement
   * will be executed with. <b>You only need this for simple statements, and if you use custom
   * codecs.</b> Bound statements already know which session they are attached to.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setPagingState(@Nullable PagingState newPagingState, @Nullable Session session) {
    if (newPagingState == null) {
      return setPagingState((ByteBuffer) null);
    } else if (newPagingState.matches(this, session)) {
      return setPagingState(newPagingState.getRawPagingState());
    } else {
      throw new IllegalArgumentException(
          "Paging state mismatch, "
              + "this means that either the paging state contents were altered, "
              + "or you're trying to apply it to a different statement");
    }
  }

  /**
   * Returns the page size to use for the statement.
   *
   * @return the set page size, otherwise 0 or a negative value to use the default value defined in
   *     the configuration.
   * @see DefaultDriverOption#REQUEST_PAGE_SIZE
   */
  int getPageSize();

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #getPageSize()}.
   */
  @Deprecated
  default int getFetchSize() {
    return getPageSize();
  }

  /**
   * Configures how many rows will be retrieved simultaneously in a single network roundtrip (the
   * goal being to avoid loading too many results in memory at the same time).
   *
   * @param newPageSize the page size to use, set to 0 or a negative value to use the default value
   *     defined in the configuration.
   * @see DefaultDriverOption#REQUEST_PAGE_SIZE
   */
  @NonNull
  @CheckReturnValue
  SelfT setPageSize(int newPageSize);

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #setPageSize(int)}.
   */
  @Deprecated
  @NonNull
  @CheckReturnValue
  default SelfT setFetchSize(int newPageSize) {
    return setPageSize(newPageSize);
  }

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
  @NonNull
  @CheckReturnValue
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
  @CheckReturnValue
  SelfT setSerialConsistencyLevel(@Nullable ConsistencyLevel newSerialConsistencyLevel);

  /** Whether tracing information should be recorded for this statement. */
  boolean isTracing();

  /**
   * A custom "now in seconds" to use when applying the request (for testing purposes).
   *
   * <p>This method's default implementation returns {@link #NO_NOW_IN_SECONDS}. The only reason it
   * exists is to preserve binary compatibility. Internally, the driver overrides it to return the
   * value that was set programmatically (if any).
   *
   * @see #NO_NOW_IN_SECONDS
   */
  default int getNowInSeconds() {
    return NO_NOW_IN_SECONDS;
  }

  /**
   * Sets the "now in seconds" to use when applying the request (for testing purposes).
   *
   * <p>This method's default implementation returns the statement unchanged. The only reason it
   * exists is to preserve binary compatibility. Internally, the driver overrides it to record the
   * new value.
   *
   * @see #NO_NOW_IN_SECONDS
   */
  @NonNull
  @SuppressWarnings("unchecked")
  default SelfT setNowInSeconds(int nowInSeconds) {
    return (SelfT) this;
  }

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
  @CheckReturnValue
  default SelfT copy(@Nullable ByteBuffer newPagingState) {
    return setPagingState(newPagingState);
  }
}
