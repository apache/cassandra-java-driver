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
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.internal.mapper.DefaultStatementAttributes;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

public class StatementAttributesBuilder {

  private DriverExecutionProfile profile = null;
  private String profileName = null;
  private Boolean isIdempotent = null;
  private int pageSize = Integer.MIN_VALUE;
  private ConsistencyLevel consistencyLevel = null;
  private ConsistencyLevel serialConsistencyLevel = null;
  private Duration timeout = null;
  private ByteBuffer routingKey = null;
  private String routingKeyspace = null;
  private Token routingToken = null;
  private boolean tracing = false;
  private ByteBuffer pagingState = null;
  private long queryTimestamp = Long.MIN_VALUE;
  private Node node = null;
  private Map<String, ByteBuffer> customPayload = NullAllowingImmutableMap.of();

  /**
   * If this is not called, it will default to {@code null}, meaning no profile.
   *
   * @see StatementAttributes#getExecutionProfile()
   */
  @NonNull
  public StatementAttributesBuilder withExecutionProfile(@Nullable DriverExecutionProfile profile) {
    this.profile = profile;
    return this;
  }

  /**
   * If this is not called, it will default to {@code null}, meaning no profile name.
   *
   * @see StatementAttributes#getExecutionProfileName()
   */
  @NonNull
  public StatementAttributesBuilder withExecutionProfileName(@Nullable String profileName) {
    this.profileName = profileName;
    return this;
  }

  /**
   * If this is not called, it will default to {@code null}, meaning use the default value defined
   * in the configuration.
   *
   * @see StatementAttributes#isIdempotent()
   */
  @NonNull
  public StatementAttributesBuilder withIdempotent(boolean idempotent) {
    this.isIdempotent = idempotent;
    return this;
  }

  /**
   * If this is not called, it will default to a negative value, meaning use the default value
   * defined in the configuration.
   *
   * @see StatementAttributes#getPageSize()
   */
  @NonNull
  public StatementAttributesBuilder withPageSize(int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  /**
   * If this is not called, it will default to {@code null}, meaning use the default value defined
   * in the configuration.
   *
   * @see StatementAttributes#getConsistencyLevel()
   */
  @NonNull
  public StatementAttributesBuilder withConsistencyLevel(
      @Nullable ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
    return this;
  }

  /**
   * If this is not called, it will default to {@code null}, meaning use the default value defined
   * in the configuration.
   *
   * @see StatementAttributes#getSerialConsistencyLevel()
   */
  @NonNull
  public StatementAttributesBuilder withSerialConsistencyLevel(
      @Nullable ConsistencyLevel serialConsistencyLevel) {
    this.serialConsistencyLevel = serialConsistencyLevel;
    return this;
  }

  /**
   * If this is not called, it will default to {@code null}, meaning use the default value defined
   * in the configuration.
   *
   * @see StatementAttributes#getTimeout()
   */
  @NonNull
  public StatementAttributesBuilder withTimeout(@Nullable Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  /**
   * If this is not called, it will default to {@code null}, meaning no routing key.
   *
   * @see StatementAttributes#getRoutingKey()
   */
  @NonNull
  public StatementAttributesBuilder withRoutingKey(@Nullable ByteBuffer routingKey) {
    this.routingKey = routingKey;
    return this;
  }

  /**
   * If this is not called, it will default to {@code null}, meaning no routing keyspace.
   *
   * @see StatementAttributes#getRoutingKeyspace()
   */
  @NonNull
  public StatementAttributesBuilder withRoutingKeyspace(@Nullable String routingKeyspace) {
    this.routingKeyspace = routingKeyspace;
    return this;
  }

  /**
   * If this is not called, it will default to {@code null}, meaning no routing token.
   *
   * @see StatementAttributes#getRoutingToken()
   */
  @NonNull
  public StatementAttributesBuilder withRoutingToken(@Nullable Token routingToken) {
    this.routingToken = routingToken;
    return this;
  }

  /**
   * If this is not called, it will default to {@code false}.
   *
   * @see StatementAttributes#isTracing()
   */
  @NonNull
  public StatementAttributesBuilder withTracing(boolean tracing) {
    this.tracing = tracing;
    return this;
  }

  /**
   * If this is not called, it will default to {@code null}, meaning no paging state.
   *
   * @see StatementAttributes#getPagingState()
   */
  @NonNull
  public StatementAttributesBuilder withPagingState(@Nullable ByteBuffer pagingState) {
    this.pagingState = pagingState;
    return this;
  }

  /**
   * If this is not called, it will default to {@code Long.MIN_VALUE}, meaning defer to the
   * timestamp generator.
   *
   * @see StatementAttributes#getQueryTimestamp()
   */
  @NonNull
  public StatementAttributesBuilder withQueryTimestamp(long queryTimestamp) {
    this.queryTimestamp = queryTimestamp;
    return this;
  }

  /**
   * If this is not called, it will default to {@code null}, meaning no targeted node.
   *
   * @see StatementAttributes#getNode()
   */
  @NonNull
  public StatementAttributesBuilder withNode(@Nullable Node node) {
    this.node = node;
    return this;
  }

  /**
   * If this is not called, it will default to an empty map, meaning no custom payload.
   *
   * @see StatementAttributes#getCustomPayload()
   */
  @NonNull
  public StatementAttributesBuilder withCustomPayload(
      @NonNull Map<String, ByteBuffer> customPayload) {
    this.customPayload = customPayload;
    return this;
  }

  @NonNull
  public StatementAttributes build() {
    return new DefaultStatementAttributes(
        profile,
        profileName,
        isIdempotent,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        routingKey,
        routingKeyspace,
        routingToken,
        tracing,
        pagingState,
        queryTimestamp,
        node,
        customPayload);
  }
}
