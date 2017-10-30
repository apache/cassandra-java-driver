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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import java.nio.ByteBuffer;
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
   * The name of the driver configuration profile that will be used for execution.
   *
   * <p>Note that this will be ignored if {@link #getConfigProfile()} returns a non-null value.
   *
   * @see DriverConfig
   */
  String getConfigProfileName();

  /**
   * The configuration profile to use for execution.
   *
   * <p>It is generally simpler to specify a profile name with {@link #getConfigProfileName()}.
   * However, this method can be used to provide a "derived" profile that was built programatically
   * by the client code. If specified, it overrides the profile name.
   *
   * @see DriverConfigProfile
   */
  DriverConfigProfile getConfigProfile();

  /**
   * The CQL keyspace to execute this request in.
   *
   * <p>This overrides {@link Session#getKeyspace()} for this particular request, providing a way to
   * specify the keyspace without forcing it globally on the session, nor hard-coding it in the
   * query string.
   *
   * <p>This feature is only available with {@link CoreProtocolVersion#V5 native protocol v5} or
   * higher. Specifying a per-request keyspace with lower protocol versions will cause a runtime
   * error.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-10145">CASSANDRA-10145</a>
   */
  CqlIdentifier getKeyspace();

  /**
   * The keyspace to use for token-aware routing, if no {@link #getKeyspace() per-request keyspace
   * is defined}.
   *
   * <p>See {@link #getRoutingKey()} for a detailed explanation of token-aware routing.
   *
   * <p>Note that this is the only way to define a routing keyspace for protocol v4 or lower.
   */
  CqlIdentifier getRoutingKeyspace();

  /**
   * The (encoded) partition key to use for token-aware routing.
   *
   * <p>When the driver picks a coordinator to execute a request, it prioritizes the replicas of the
   * partition that this query operates on, in order to avoid an extra network jump on the server
   * side. To find these replicas, it needs a keyspace (which is where the replication settings are
   * defined) and a key, that are computed the following way:
   *
   * <ul>
   *   <li>if a per-request keyspace is specified with {@link #getKeyspace()}, it is used as the
   *       keyspace;
   *   <li>otherwise, if {@link #getRoutingKeyspace()} is specified, it is used as the keyspace;
   *   <li>otherwise, if {@link Session#getKeyspace()} is not null, it is used as the keyspace;
   *   <li>if a routing token is defined with {@link #getRoutingToken()}, it is used as the key;
   *   <li>otherwise, the result of this method is used as the key.
   * </ul>
   *
   * If either keyspace or key is null at the end of this process, then token-aware routing is
   * disabled.
   */
  ByteBuffer getRoutingKey();

  /**
   * The token to use for token-aware routing.
   *
   * <p>This is the same information as {@link #getRoutingKey()}, but already hashed in a token. It
   * is probably more useful for analytics tools that "shard" a query on a set of token ranges.
   *
   * <p>See {@link #getRoutingKey()} for a detailed explanation of token-aware routing.
   */
  Token getRoutingToken();

  /**
   * Returns the custom payload to send alongside the request.
   *
   * <p>This is used to exchange extra information with the server. By default, Cassandra doesn't do
   * anything with this, you'll only need it if you have a custom request handler on the
   * server-side.
   */
  Map<String, ByteBuffer> getCustomPayload();

  /**
   * Whether the request is idempotent; that is, whether applying the request twice yields the same
   * result.
   *
   * <p>This is used internally for retries and speculative executions: if a request is not
   * idempotent, the driver will take extra care to ensure that it is not sent twice (for example,
   * don't retry if there is the slightest chance that the request reached a coordinator).
   *
   * @return a boolean value, or {@code null} to use the default value defined in the configuration.
   * @see CoreDriverOption#REQUEST_DEFAULT_IDEMPOTENCE
   */
  Boolean isIdempotent();

  /**
   * Whether tracing information should be recorded for this request.
   *
   * <p>Tracing is rather specific to CQL, but this is exposed in this interface because it is
   * available at the protocol level. Request implementations are free to use it if it is relevant
   * to them, or always return {@code false} otherwise.
   */
  boolean isTracing();
}
