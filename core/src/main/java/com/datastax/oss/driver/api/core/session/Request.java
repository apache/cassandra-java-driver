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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Partitioner;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

/**
 * A request executed by a {@link Session}.
 *
 * <p>This is a high-level abstraction, agnostic to the actual language (e.g. CQL). A request is
 * anything that can be converted to a protocol message, provided that you register a request
 * processor with the driver to do that conversion.
 */
public interface Request {

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
   * The CQL keyspace to execute this request in, or {@code null} if this request does not specify
   * any keyspace.
   *
   * <p>This overrides {@link Session#getKeyspace()} for this particular request, providing a way to
   * specify the keyspace without forcing it globally on the session, nor hard-coding it in the
   * query string.
   *
   * <p>This feature is only available with {@link DefaultProtocolVersion#V5 native protocol v5} or
   * higher. Specifying a per-request keyspace with lower protocol versions will cause a runtime
   * error.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-10145">CASSANDRA-10145</a>
   */
  @Nullable
  CqlIdentifier getKeyspace();

  /**
   * The keyspace to use for token-aware routing.
   *
   * <p>Note that if a {@linkplain #getKeyspace() per-request keyspace} is already defined for this
   * request, it takes precedence over this method.
   *
   * <p>See {@link #getRoutingKey()} for a detailed explanation of token-aware routing.
   */
  @Nullable
  CqlIdentifier getRoutingKeyspace();

  /**
   * The table to use for tablet-aware routing. Infers the table from available ColumnDefinitions or
   * {@code null} if it is not possible.
   *
   * @return
   */
  @Nullable
  default CqlIdentifier getRoutingTable() {
    return null;
  }

  /**
   * The partition key to use for token-aware routing.
   *
   * <p>For each request, the driver tries to determine a <em>routing keyspace</em> and a
   * <em>routing key</em> by calling the following methods:
   *
   * <ul>
   *   <li>routing keyspace:
   *       <ul>
   *         <li>the result of {@link #getKeyspace()}, if not null;
   *         <li>otherwise, the result of {@link #getRoutingKeyspace()}, if not null;
   *         <li>otherwise, the result of {@link Session#getKeyspace()}, if not empty;
   *         <li>otherwise, null.
   *       </ul>
   *   <li>routing key:
   *       <ul>
   *         <li>the result of {@link #getRoutingToken()}, if not null;
   *         <li>otherwise, the result of {@link #getRoutingKey()}, if not null;
   *         <li>otherwise, null.
   *       </ul>
   * </ul>
   *
   * This provides a hint of the partition that the request operates on. When the driver picks a
   * coordinator for execution, it will prioritize the replicas that own that partition, in order to
   * avoid an extra network jump on the server side.
   *
   * <p>Routing information is optional: if either keyspace or key is null, token-aware routing is
   * disabled for this request.
   */
  @Nullable
  ByteBuffer getRoutingKey();

  /**
   * The token to use for token-aware routing.
   *
   * <p>This is an alternative to {@link #getRoutingKey()}. Both methods represent the same
   * information, a request can provide one or the other.
   *
   * <p>See {@link #getRoutingKey()} for a detailed explanation of token-aware routing.
   */
  @Nullable
  Token getRoutingToken();

  /**
   * The partitioner to use for token-aware routing. If {@code null}, the cluster-wide partitioner
   * will be used.
   */
  default Partitioner getPartitioner() {
    return null;
  }

  /**
   * Returns the custom payload to send alongside the request.
   *
   * <p>This is used to exchange extra information with the server. By default, Cassandra doesn't do
   * anything with this, you'll only need it if you have a custom request handler on the
   * server-side.
   *
   * @return The custom payload, or an empty map if no payload is present.
   */
  @NonNull
  Map<String, ByteBuffer> getCustomPayload();

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
   * How long to wait for this request to complete. This is a global limit on the duration of a
   * session.execute() call, including any retries the driver might do.
   *
   * @return the set duration, or {@code null} to use the default value defined in the
   *     configuration.
   * @see DefaultDriverOption#REQUEST_TIMEOUT
   */
  @Nullable
  Duration getTimeout();

  /** @return The node configured on this statement, or null if none is configured. */
  @Nullable
  Node getNode();
}
