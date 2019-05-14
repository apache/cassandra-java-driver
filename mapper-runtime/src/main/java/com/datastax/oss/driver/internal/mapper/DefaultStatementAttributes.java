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
package com.datastax.oss.driver.internal.mapper;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.mapper.StatementAttributes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

public class DefaultStatementAttributes implements StatementAttributes {

  private final DriverExecutionProfile profile;
  private final String profileName;
  private final Boolean isIdempotent;
  private final int pageSize;
  private final ConsistencyLevel consistencyLevel;
  private final ConsistencyLevel serialConsistencyLevel;
  private final Duration timeout;
  private final ByteBuffer routingKey;
  private final String routingKeyspace;
  private final Token routingToken;
  private final boolean tracing;
  private final ByteBuffer pagingState;
  private final long queryTimestamp;
  private final Node node;
  private final Map<String, ByteBuffer> customPayload;

  public DefaultStatementAttributes(
      DriverExecutionProfile profile,
      String profileName,
      Boolean isIdempotent,
      int pageSize,
      ConsistencyLevel consistencyLevel,
      ConsistencyLevel serialConsistencyLevel,
      Duration timeout,
      ByteBuffer routingKey,
      String routingKeyspace,
      Token routingToken,
      boolean tracing,
      ByteBuffer pagingState,
      long queryTimestamp,
      Node node,
      Map<String, ByteBuffer> customPayload) {

    this.profile = profile;
    this.profileName = profileName;
    this.isIdempotent = isIdempotent;
    this.pageSize = pageSize;
    this.consistencyLevel = consistencyLevel;
    this.serialConsistencyLevel = serialConsistencyLevel;
    this.timeout = timeout;
    this.routingKey = routingKey;
    this.routingKeyspace = routingKeyspace;
    this.routingToken = routingToken;
    this.tracing = tracing;
    this.pagingState = pagingState;
    this.queryTimestamp = queryTimestamp;
    this.node = node;
    this.customPayload = customPayload;
  }

  @Nullable
  @Override
  public DriverExecutionProfile getExecutionProfile() {
    return profile;
  }

  @Nullable
  @Override
  public String getExecutionProfileName() {
    return profileName;
  }

  @Nullable
  @Override
  public Boolean isIdempotent() {
    return isIdempotent;
  }

  @Override
  public int getPageSize() {
    return pageSize;
  }

  @Nullable
  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  @Nullable
  @Override
  public ConsistencyLevel getSerialConsistencyLevel() {
    return serialConsistencyLevel;
  }

  @Nullable
  @Override
  public Duration getTimeout() {
    return timeout;
  }

  @Nullable
  @Override
  public ByteBuffer getRoutingKey() {
    return routingKey;
  }

  @Nullable
  @Override
  public String getRoutingKeyspace() {
    return routingKeyspace;
  }

  @Nullable
  @Override
  public Token getRoutingToken() {
    return routingToken;
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @Nullable
  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @Override
  public long getQueryTimestamp() {
    return queryTimestamp;
  }

  @Nullable
  @Override
  public Node getNode() {
    return node;
  }

  @NonNull
  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }
}
