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
package com.datastax.oss.driver.api.mapper;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

/**
 * A set of runtime parameters to use for mapped queries.
 *
 * <p>If you pass an instance of this type as the last argument of any DAO method, it will be
 * applied to the generated statement.
 */
public interface StatementAttributes {

  /**
   * The execution profile to use for this request, or {@code null} if no profile has been set.
   *
   * <p>It is generally simpler to specify a profile name with {@link #getExecutionProfileName()}.
   * However, this method can be used to provide a "derived" profile that was built programmatically
   * by the client code. If specified, it overrides the profile name.
   *
   * @see DriverExecutionProfile
   */
  @Nullable
  DriverExecutionProfile getExecutionProfile();

  /**
   * The name of the execution profile that will be used for this request, or {@code null} if no
   * profile has been set.
   *
   * <p>Note that this will be ignored if {@link #getExecutionProfile()} returns a non-null value.
   *
   * @see DriverConfig
   */
  @Nullable
  String getExecutionProfileName();

  /**
   * Whether the request is idempotent; that is, whether applying the request twice leaves the
   * database in the same state.
   *
   * <p>This is used internally for retries and speculative executions: if a request is not
   * idempotent, the driver will take extra care to ensure that it is not sent twice (for example,
   * don't retry if there is the slightest chance that the request reached a coordinator).
   *
   * @return a boolean value, or {@code null} to use the default value defined in the configuration.
   * @see DefaultDriverOption#REQUEST_DEFAULT_IDEMPOTENCE
   */
  @Nullable
  Boolean isIdempotent();

  /**
   * The page size to use for the statement.
   *
   * @return the set page size, otherwise 0 or a negative value to use the default value defined in
   *     the configuration.
   * @see DefaultDriverOption#REQUEST_PAGE_SIZE
   */
  int getPageSize();

  /**
   * The {@link ConsistencyLevel} to use for the statement.
   *
   * @return the set consistency, or {@code null} to use the default value defined in the
   *     configuration.
   * @see DefaultDriverOption#REQUEST_CONSISTENCY
   */
  @Nullable
  ConsistencyLevel getConsistencyLevel();

  /**
   * The serial {@link ConsistencyLevel} to use for the statement.
   *
   * @return the set serial consistency, or {@code null} to use the default value defined in the
   *     configuration.
   * @see DefaultDriverOption#REQUEST_SERIAL_CONSISTENCY
   */
  @Nullable
  ConsistencyLevel getSerialConsistencyLevel();

  /**
   * How long to wait for this request to complete. This is a global limit on the duration of a
   * session.execute() call, including any retries the driver might do.
   *
   * @return the set duration, or {@code null} to use the default value defined in the
   *     configuration.
   * @see DefaultDriverOption#REQUEST_TIMEOUT
   */
  @Nullable
  Duration getTimeout();

  /**
   * The keyspace to use for token-aware routing, {@code null} if this request does not use
   * token-aware routing.
   *
   * <p>See {@link #getRoutingKey()} for a detailed explanation of token-aware routing.
   *
   * <p>Note that this is the only way to define a routing keyspace for protocol v4 or lower.
   */
  @Nullable
  String getRoutingKeyspace();

  /**
   * The (encoded) partition key to use for token-aware routing, or {@code null} if this request
   * does not use token-aware routing.
   *
   * <p>When the driver picks a coordinator to execute a request, it prioritizes the replicas of the
   * partition that this query operates on, in order to avoid an extra network jump on the server
   * side. To find these replicas, it needs a keyspace (which is where the replication settings are
   * defined) and a key, that are computed the following way:
   *
   * <ul>
   *   <li>otherwise, if {@link #getRoutingKeyspace()} is specified, it is used as the keyspace;
   *   <li>otherwise, if {@link Session#getKeyspace()} is not {@code null}, it is used as the
   *       keyspace;
   *   <li>if a routing token is defined with {@link #getRoutingToken()}, it is used as the key;
   *   <li>otherwise, the result of this method is used as the key.
   * </ul>
   *
   * If either keyspace or key is {@code null} at the end of this process, then token-aware routing
   * is disabled.
   */
  @Nullable
  ByteBuffer getRoutingKey();

  /**
   * The token to use for token-aware routing, or {@code null} if this request does not use
   * token-aware routing.
   *
   * <p>This is the same information as {@link #getRoutingKey()}, but already hashed in a token. It
   * is probably more useful for analytics tools that "shard" a query on a set of token ranges.
   *
   * <p>See {@link #getRoutingKey()} for a detailed explanation of token-aware routing.
   */
  @Nullable
  Token getRoutingToken();

  /** Whether tracing information should be recorded for this statement. */
  boolean isTracing();

  /**
   * The paging state to send with the statement, or {@code null} if this statement has no paging
   * state.
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
   * The query timestamp, in microseconds, to send with the statement.
   *
   * <p>If this is equal to {@link Long#MIN_VALUE}, the {@link TimestampGenerator} configured for
   * this driver instance will be used to generate a timestamp.
   *
   * @see TimestampGenerator
   */
  long getQueryTimestamp();

  /**
   * The {@link Node} that should handle this query.
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
   */
  @Nullable
  Node getNode();

  /**
   * The custom payload to send alongside the request.
   *
   * <p>This is used to exchange extra information with the server. By default, Cassandra doesn't do
   * anything with this, you'll only need it if you have a custom request handler on the
   * server-side.
   *
   * @return The custom payload, or an empty map if no payload is present.
   */
  @NonNull
  Map<String, ByteBuffer> getCustomPayload();

  /** Creates a builder to construct an instance of this type. */
  static StatementAttributesBuilder builder() {
    return new StatementAttributesBuilder();
  }
}
