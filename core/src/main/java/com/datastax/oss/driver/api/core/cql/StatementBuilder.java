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
import com.datastax.oss.driver.internal.core.util.RoutingKey;
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
  protected long timestamp = Statement.NO_DEFAULT_TIMESTAMP;
  @Nullable protected ByteBuffer pagingState;
  protected int pageSize = Integer.MIN_VALUE;
  @Nullable protected ConsistencyLevel consistencyLevel;
  @Nullable protected ConsistencyLevel serialConsistencyLevel;
  @Nullable protected Duration timeout;
  @Nullable protected Node node;
  protected int nowInSeconds = Statement.NO_NOW_IN_SECONDS;

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
    this.timestamp = template.getQueryTimestamp();
    this.pagingState = template.getPagingState();
    this.pageSize = template.getPageSize();
    this.consistencyLevel = template.getConsistencyLevel();
    this.serialConsistencyLevel = template.getSerialConsistencyLevel();
    this.timeout = template.getTimeout();
    this.node = template.getNode();
    this.nowInSeconds = template.getNowInSeconds();
  }

  /** @see Statement#setExecutionProfileName(String) */
  @NonNull
  public SelfT setExecutionProfileName(@Nullable String executionProfileName) {
    this.executionProfileName = executionProfileName;
    if (executionProfileName != null) {
      this.executionProfile = null;
    }
    return self;
  }

  /** @see Statement#setExecutionProfile(DriverExecutionProfile) */
  @NonNull
  public SelfT setExecutionProfile(@Nullable DriverExecutionProfile executionProfile) {
    this.executionProfile = executionProfile;
    if (executionProfile != null) {
      this.executionProfileName = null;
    }
    return self;
  }

  /** @see Statement#setRoutingKeyspace(CqlIdentifier) */
  @NonNull
  public SelfT setRoutingKeyspace(@Nullable CqlIdentifier routingKeyspace) {
    this.routingKeyspace = routingKeyspace;
    return self;
  }

  /**
   * Shortcut for {@link #setRoutingKeyspace(CqlIdentifier)
   * setRoutingKeyspace(CqlIdentifier.fromCql(routingKeyspaceName))}.
   */
  @NonNull
  public SelfT setRoutingKeyspace(@Nullable String routingKeyspaceName) {
    return setRoutingKeyspace(
        routingKeyspaceName == null ? null : CqlIdentifier.fromCql(routingKeyspaceName));
  }

  /** @see Statement#setRoutingKey(ByteBuffer) */
  @NonNull
  public SelfT setRoutingKey(@Nullable ByteBuffer routingKey) {
    this.routingKey = routingKey;
    return self;
  }

  /** @see Statement#setRoutingKey(ByteBuffer...) */
  @NonNull
  public SelfT setRoutingKey(@NonNull ByteBuffer... newRoutingKeyComponents) {
    return setRoutingKey(RoutingKey.compose(newRoutingKeyComponents));
  }

  /** @see Statement#setRoutingToken(Token) */
  @NonNull
  public SelfT setRoutingToken(@Nullable Token routingToken) {
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
  public SelfT setIdempotence(@Nullable Boolean idempotent) {
    this.idempotent = idempotent;
    return self;
  }

  /**
   * This method is a shortcut to {@link #setTracing(boolean)} with an argument of true. It is
   * preserved to maintain API compatibility.
   *
   * @see Statement#setTracing(boolean)
   */
  @NonNull
  public SelfT setTracing() {
    return setTracing(true);
  }

  /** @see Statement#setTracing(boolean) */
  @NonNull
  public SelfT setTracing(boolean tracing) {
    this.tracing = tracing;
    return self;
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #setTracing(boolean) setTracing(true)}.
   */
  @Deprecated
  @NonNull
  public SelfT enableTracing() {
    return setTracing(true);
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #setTracing(boolean) setTracing(false)}.
   */
  @Deprecated
  @NonNull
  public SelfT disableTracing() {
    return setTracing(false);
  }

  /** @see Statement#setQueryTimestamp(long) */
  @NonNull
  public SelfT setQueryTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return self;
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #setQueryTimestamp(long)}.
   */
  @Deprecated
  @NonNull
  public SelfT setDefaultTimestamp(long timestamp) {
    return setQueryTimestamp(timestamp);
  }

  /** @see Statement#setPagingState(ByteBuffer) */
  @NonNull
  public SelfT setPagingState(@Nullable ByteBuffer pagingState) {
    this.pagingState = pagingState;
    return self;
  }

  /** @see Statement#setPageSize(int) */
  @NonNull
  public SelfT setPageSize(int pageSize) {
    this.pageSize = pageSize;
    return self;
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #setPageSize(int)}.
   */
  @Deprecated
  @NonNull
  public SelfT setFetchSize(int pageSize) {
    return this.setPageSize(pageSize);
  }

  /** @see Statement#setConsistencyLevel(ConsistencyLevel) */
  @NonNull
  public SelfT setConsistencyLevel(@Nullable ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
    return self;
  }

  /** @see Statement#setSerialConsistencyLevel(ConsistencyLevel) */
  @NonNull
  public SelfT setSerialConsistencyLevel(@Nullable ConsistencyLevel serialConsistencyLevel) {
    this.serialConsistencyLevel = serialConsistencyLevel;
    return self;
  }

  /** @see Statement#setTimeout(Duration) */
  @NonNull
  public SelfT setTimeout(@Nullable Duration timeout) {
    this.timeout = timeout;
    return self;
  }

  /** @see Statement#setNode(Node) */
  public SelfT setNode(@Nullable Node node) {
    this.node = node;
    return self;
  }

  /** @see Statement#setNowInSeconds(int) */
  public SelfT setNowInSeconds(int nowInSeconds) {
    this.nowInSeconds = nowInSeconds;
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
