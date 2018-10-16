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
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;

/**
 * Handle options common to all statement builders.
 *
 * @see SimpleStatement#builder(String)
 * @see BatchStatement#builder(BatchType)
 * @see PreparedStatement#boundStatementBuilder(Object...)
 */
@NotThreadSafe
public abstract class StatementBuilder<
    SelfT extends StatementBuilder<SelfT, StatementT>, StatementT extends Statement<StatementT>> {

  @SuppressWarnings("unchecked")
  private final SelfT self = (SelfT) this;

  @Nullable protected String executionProfileName;
  @Nullable protected DriverExecutionProfile executionProfile;
  @Nullable protected CqlIdentifier routingKeyspace;
  @Nullable protected ByteBuffer routingKey;
  @Nullable protected Token routingToken;
  @Nullable private NullAllowingImmutableMap.Builder<String, ByteBuffer> customPayloadBuilder;
  @Nullable protected Boolean idempotent;
  protected boolean tracing;
  protected long timestamp = Long.MIN_VALUE;
  @Nullable protected ByteBuffer pagingState;
  protected int pageSize = Integer.MIN_VALUE;
  @Nullable protected ConsistencyLevel consistencyLevel;
  @Nullable protected ConsistencyLevel serialConsistencyLevel;
  @Nullable protected Duration timeout;
  @Nullable protected Node node;

  protected StatementBuilder() {
    // nothing to do
  }

  protected StatementBuilder(StatementT template) {
    this.executionProfileName = template.getExecutionProfileName();
    this.executionProfile = template.getExecutionProfile();
    this.routingKeyspace = template.getRoutingKeyspace();
    this.routingKey = template.getRoutingKey();
    this.routingToken = template.getRoutingToken();
    if (!template.getCustomPayload().isEmpty()) {
      this.customPayloadBuilder =
          NullAllowingImmutableMap.<String, ByteBuffer>builder()
              .putAll(template.getCustomPayload());
    }
    this.idempotent = template.isIdempotent();
    this.tracing = template.isTracing();
    this.timestamp = template.getTimestamp();
    this.pagingState = template.getPagingState();
    this.pageSize = template.getPageSize();
    this.consistencyLevel = template.getConsistencyLevel();
    this.serialConsistencyLevel = template.getSerialConsistencyLevel();
    this.timeout = template.getTimeout();
    this.node = template.getNode();
  }

  /** @see Statement#setExecutionProfileName(String) */
  @NonNull
  public SelfT withExecutionProfileName(@Nullable String executionProfileName) {
    this.executionProfileName = executionProfileName;
    return self;
  }

  /** @see Statement#setExecutionProfile(DriverExecutionProfile) */
  @NonNull
  public SelfT withExecutionProfile(@Nullable DriverExecutionProfile executionProfile) {
    this.executionProfile = executionProfile;
    this.executionProfileName = null;
    return self;
  }

  /** @see Statement#setRoutingKeyspace(CqlIdentifier) */
  @NonNull
  public SelfT withRoutingKeyspace(@Nullable CqlIdentifier routingKeyspace) {
    this.routingKeyspace = routingKeyspace;
    return self;
  }

  /**
   * Shortcut for {@link #withRoutingKeyspace(CqlIdentifier)
   * withRoutingKeyspace(CqlIdentifier.fromCql(routingKeyspaceName))}.
   */
  @NonNull
  public SelfT withRoutingKeyspace(@Nullable String routingKeyspaceName) {
    return withRoutingKeyspace(
        routingKeyspaceName == null ? null : CqlIdentifier.fromCql(routingKeyspaceName));
  }

  /** @see Statement#setRoutingKey(ByteBuffer) */
  @NonNull
  public SelfT withRoutingKey(@Nullable ByteBuffer routingKey) {
    this.routingKey = routingKey;
    return self;
  }

  /** @see Statement#setRoutingToken(Token) */
  @NonNull
  public SelfT withRoutingToken(@Nullable Token routingToken) {
    this.routingToken = routingToken;
    return self;
  }

  /** @see Statement#setCustomPayload(Map) */
  @NonNull
  public SelfT addCustomPayload(@NonNull String key, @Nullable ByteBuffer value) {
    if (customPayloadBuilder == null) {
      customPayloadBuilder = NullAllowingImmutableMap.builder();
    }
    customPayloadBuilder.put(key, value);
    return self;
  }

  /** @see Statement#setCustomPayload(Map) */
  @NonNull
  public SelfT clearCustomPayload() {
    customPayloadBuilder = null;
    return self;
  }

  /** @see Statement#setIdempotent(Boolean) */
  @NonNull
  public SelfT withIdempotence(@Nullable Boolean idempotent) {
    this.idempotent = idempotent;
    return self;
  }

  /** @see Statement#setTracing(boolean) */
  @NonNull
  public SelfT withTracing() {
    this.tracing = true;
    return self;
  }

  /** @see Statement#setTimestamp(long) */
  @NonNull
  public SelfT withTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return self;
  }

  /** @see Statement#setPagingState(ByteBuffer) */
  @NonNull
  public SelfT withPagingState(@Nullable ByteBuffer pagingState) {
    this.pagingState = pagingState;
    return self;
  }

  /** @see Statement#setPageSize(int) */
  @NonNull
  public SelfT withPageSize(int pageSize) {
    this.pageSize = pageSize;
    return self;
  }

  /** @see Statement#setConsistencyLevel(ConsistencyLevel) */
  @NonNull
  public SelfT withConsistencyLevel(@Nullable ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
    return self;
  }

  /** @see Statement#setSerialConsistencyLevel(ConsistencyLevel) */
  @NonNull
  public SelfT withSerialConsistencyLevel(@Nullable ConsistencyLevel serialConsistencyLevel) {
    this.serialConsistencyLevel = serialConsistencyLevel;
    return self;
  }

  /** @see Statement#setTimeout(Duration) */
  @NonNull
  public SelfT withTimeout(@Nullable Duration timeout) {
    this.timeout = timeout;
    return self;
  }

  /** @see Statement#setNode(Node) */
  public SelfT withNode(@Nullable Node node) {
    this.node = node;
    return self;
  }

  @NonNull
  protected Map<String, ByteBuffer> buildCustomPayload() {
    return (customPayloadBuilder == null)
        ? NullAllowingImmutableMap.of()
        : customPayloadBuilder.build();
  }

  @NonNull
  public abstract StatementT build();
}
