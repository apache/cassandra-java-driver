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
package com.datastax.dse.driver.api.core.graph;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * A request to execute a DSE Graph query.
 *
 * @param <SelfT> the "self type" used for covariant returns in subtypes.
 */
public interface GraphStatement<SelfT extends GraphStatement<SelfT>> extends Request {

  /**
   * The type returned when a graph statement is executed synchronously.
   *
   * <p>Most users won't use this explicitly. It is needed for the generic execute method ({@link
   * Session#execute(Request, GenericType)}), but graph statements will generally be run with one of
   * the DSE driver's built-in helper methods (such as {@link CqlSession#execute(GraphStatement)}).
   */
  GenericType<GraphResultSet> SYNC = GenericType.of(GraphResultSet.class);

  /**
   * The type returned when a graph statement is executed asynchronously.
   *
   * <p>Most users won't use this explicitly. It is needed for the generic execute method ({@link
   * Session#execute(Request, GenericType)}), but graph statements will generally be run with one of
   * the DSE driver's built-in helper methods (such as {@link
   * CqlSession#executeAsync(GraphStatement)}).
   */
  GenericType<CompletionStage<AsyncGraphResultSet>> ASYNC =
      new GenericType<CompletionStage<AsyncGraphResultSet>>() {};

  /**
   * Set the idempotence to use for execution.
   *
   * <p>Idempotence defines whether it will be possible to speculatively re-execute the statement,
   * based on a {@link SpeculativeExecutionPolicy}.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @param idempotent a boolean instance to set a statement-specific value, or {@code null} to use
   *     the default idempotence defined in the configuration.
   */
  @NonNull
  @CheckReturnValue
  SelfT setIdempotent(@Nullable Boolean idempotent);

  /**
   * {@inheritDoc}
   *
   * <p>Note that, if this method returns {@code null}, graph statements fall back to a dedicated
   * configuration option: {@code basic.graph.timeout}. See {@code reference.conf} in the DSE driver
   * distribution for more details.
   */
  @Nullable
  @Override
  Duration getTimeout();

  /**
   * Sets how long to wait for this request to complete. This is a global limit on the duration of a
   * session.execute() call, including any retries the driver might do.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @param newTimeout the timeout to use, or {@code null} to use the default value defined in the
   *     configuration.
   * @see #getTimeout()
   */
  @NonNull
  @CheckReturnValue
  SelfT setTimeout(@Nullable Duration newTimeout);

  /**
   * Sets the {@link Node} that should handle this query.
   *
   * <p>In the general case, use of this method is <em>heavily discouraged</em> and should only be
   * used in specific cases, such as applying a series of schema changes, which may be advantageous
   * to execute in sequence on the same node.
   *
   * <p>Configuring a specific node causes the configured {@link LoadBalancingPolicy} to be
   * completely bypassed. However, if the load balancing policy dictates that the node is at
   * distance {@link NodeDistance#IGNORED} or there is no active connectivity to the node, the
   * request will fail with a {@link NoNodeAvailableException}.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @param newNode The node that should be used to handle executions of this statement or null to
   *     delegate to the configured load balancing policy.
   */
  @NonNull
  @CheckReturnValue
  SelfT setNode(@Nullable Node newNode);

  /**
   * Get the timestamp set on the statement.
   *
   * <p>By default, if left unset, the value returned by this is {@code Long.MIN_VALUE}, which means
   * that the timestamp will be set via the Timestamp Generator.
   *
   * @return the timestamp set on this statement.
   */
  long getTimestamp();

  /**
   * Set the timestamp to use for execution.
   *
   * <p>By default the timestamp generator (see reference config file) will be used for timestamps,
   * unless set explicitly via this method.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @CheckReturnValue
  SelfT setTimestamp(long timestamp);

  /**
   * Sets the configuration profile to use for execution.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  @CheckReturnValue
  SelfT setExecutionProfile(@Nullable DriverExecutionProfile executionProfile);

  /**
   * Sets the name of the driver configuration profile that will be used for execution.
   *
   * <p>For all the driver's built-in implementations, this method has no effect if {@link
   * #setExecutionProfile} has been called with a non-null argument.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  @CheckReturnValue
  SelfT setExecutionProfileName(@Nullable String name);

  /**
   * Sets the custom payload to use for execution.
   *
   * <p>This is intended for advanced use cases, such as tools with very advanced knowledge of DSE
   * Graph, and reserved for internal settings like transaction settings. Note that the driver also
   * adds graph-related options to the payload, in addition to the ones provided here; it won't
   * override any option that is already present.
   *
   * <p>All the driver's built-in statement implementations are immutable, and return a new instance
   * from this method. However custom implementations may choose to be mutable and return the same
   * instance.
   *
   * <p>Note that it's your responsibility to provide a thread-safe map. This can be achieved with a
   * concurrent or immutable implementation, or by making it effectively immutable (meaning that
   * it's never modified after being set on the statement).
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  @CheckReturnValue
  SelfT setCustomPayload(@NonNull Map<String, ByteBuffer> newCustomPayload);

  /**
   * The name of the graph to use for this statement.
   *
   * <p>This is the programmatic equivalent of the configuration option {@code basic.graph.name},
   * and takes precedence over it. That is, if this property is non-null, then the configuration
   * will be ignored.
   */
  @Nullable
  String getGraphName();

  /**
   * Sets the graph name.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #getGraphName()
   */
  @NonNull
  @CheckReturnValue
  SelfT setGraphName(@Nullable String newGraphName);

  /**
   * The name of the traversal source to use for this statement.
   *
   * <p>This is the programmatic equivalent of the configuration option {@code
   * basic.graph.traversal-source}, and takes precedence over it. That is, if this property is
   * non-null, then the configuration will be ignored.
   */
  @Nullable
  String getTraversalSource();

  /**
   * Sets the traversal source.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #getTraversalSource()
   */
  @NonNull
  @CheckReturnValue
  SelfT setTraversalSource(@Nullable String newTraversalSource);

  /**
   * The DSE graph sub-protocol to use for this statement.
   *
   * <p>This is the programmatic equivalent of the configuration option {@code
   * advanced.graph.sub-protocol}, and takes precedence over it. That is, if this property is
   * non-null, then the configuration will be ignored.
   */
  @Nullable
  String getSubProtocol();

  /**
   * Sets the sub-protocol.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #getSubProtocol()
   */
  @NonNull
  @CheckReturnValue
  SelfT setSubProtocol(@Nullable String newSubProtocol);

  /**
   * Returns the consistency level to use for the statement.
   *
   * <p>This is the programmatic equivalent of the configuration option {@code
   * basic.request.consistency}, and takes precedence over it. That is, if this property is
   * non-null, then the configuration will be ignored.
   */
  @Nullable
  ConsistencyLevel getConsistencyLevel();

  /**
   * Sets the consistency level to use for this statement.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @param newConsistencyLevel the consistency level to use, or null to use the default value
   *     defined in the configuration.
   * @see #getConsistencyLevel()
   */
  @CheckReturnValue
  SelfT setConsistencyLevel(@Nullable ConsistencyLevel newConsistencyLevel);

  /**
   * The consistency level to use for the internal read queries that will be produced by this
   * statement.
   *
   * <p>This is the programmatic equivalent of the configuration option {@code
   * basic.graph.read-consistency-level}, and takes precedence over it. That is, if this property is
   * non-null, then the configuration will be ignored.
   *
   * <p>If this property isn't set here or in the configuration, the default consistency level will
   * be used ({@link #getConsistencyLevel()} or {@code basic.request.consistency}).
   */
  @Nullable
  ConsistencyLevel getReadConsistencyLevel();

  /**
   * Sets the read consistency level.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #getReadConsistencyLevel()
   */
  @NonNull
  @CheckReturnValue
  SelfT setReadConsistencyLevel(@Nullable ConsistencyLevel newReadConsistencyLevel);

  /**
   * The consistency level to use for the internal write queries that will be produced by this
   * statement.
   *
   * <p>This is the programmatic equivalent of the configuration option {@code
   * basic.graph.write-consistency-level}, and takes precedence over it. That is, if this property
   * is non-null, then the configuration will be ignored.
   *
   * <p>If this property isn't set here or in the configuration, the default consistency level will
   * be used ({@link #getConsistencyLevel()} or {@code basic.request.consistency}).
   */
  @Nullable
  ConsistencyLevel getWriteConsistencyLevel();

  /**
   * Sets the write consistency level.
   *
   * <p>All the driver's built-in implementations are immutable, and return a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #getWriteConsistencyLevel()
   */
  @NonNull
  @CheckReturnValue
  SelfT setWriteConsistencyLevel(@Nullable ConsistencyLevel newWriteConsistencyLevel);

  /** Graph statements do not have a per-query keyspace, this method always returns {@code null}. */
  @Nullable
  @Override
  default CqlIdentifier getKeyspace() {
    return null;
  }

  /** Graph statements can't be routed, this method always returns {@code null}. */
  @Nullable
  @Override
  default CqlIdentifier getRoutingKeyspace() {
    return null;
  }

  /** Graph statements can't be routed, this method always returns {@code null}. */
  @Nullable
  @Override
  default ByteBuffer getRoutingKey() {
    return null;
  }

  /** Graph statements can't be routed, this method always returns {@code null}. */
  @Nullable
  @Override
  default Token getRoutingToken() {
    return null;
  }

  /**
   * Whether tracing information should be recorded for this statement.
   *
   * <p>This method is only exposed for future extensibility. At the time of writing, graph
   * statements do not support tracing, and this always returns {@code false}.
   */
  default boolean isTracing() {
    return false;
  }
}
