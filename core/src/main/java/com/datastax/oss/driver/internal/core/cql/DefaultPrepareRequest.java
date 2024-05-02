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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import net.jcip.annotations.Immutable;

/**
 * Default implementation of a prepare request, which is built internally to handle calls such as
 * {@link CqlSession#prepare(String)} and {@link CqlSession#prepare(SimpleStatement)}.
 *
 * <p>When built from a {@link SimpleStatement}, it propagates the attributes to bound statements
 * according to the rules described in {@link CqlSession#prepare(SimpleStatement)}. The prepare
 * request itself:
 *
 * <ul>
 *   <li>will use the same execution profile (or execution profile name) as the {@code
 *       SimpleStatement};
 *   <li>will use the same custom payload as the {@code SimpleStatement};
 *   <li>will use a {@code null} timeout in order to default to the configuration (assuming that if
 *       a statement with a custom timeout is prepared, it is intended for the bound statements, not
 *       the preparation itself).
 * </ul>
 */
@Immutable
public class DefaultPrepareRequest implements PrepareRequest {

  private final SimpleStatement statement;

  public DefaultPrepareRequest(SimpleStatement statement) {
    this.statement = statement;
  }

  public DefaultPrepareRequest(String query) {
    this.statement = SimpleStatement.newInstance(query);
  }

  @NonNull
  @Override
  public String getQuery() {
    return statement.getQuery();
  }

  @Nullable
  @Override
  public String getExecutionProfileName() {
    return statement.getExecutionProfileName();
  }

  @Nullable
  @Override
  public DriverExecutionProfile getExecutionProfile() {
    return statement.getExecutionProfile();
  }

  @Nullable
  @Override
  public CqlIdentifier getKeyspace() {
    return statement.getKeyspace();
  }

  @Nullable
  @Override
  public CqlIdentifier getRoutingKeyspace() {
    // Prepare requests do not operate on a particular partition, token-aware routing doesn't apply.
    return null;
  }

  @Nullable
  @Override
  public ByteBuffer getRoutingKey() {
    return null;
  }

  @Nullable
  @Override
  public Token getRoutingToken() {
    return null;
  }

  @NonNull
  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return statement.getCustomPayload();
  }

  @Nullable
  @Override
  public Duration getTimeout() {
    return null;
  }

  @Nullable
  @Override
  public String getExecutionProfileNameForBoundStatements() {
    return statement.getExecutionProfileName();
  }

  @Nullable
  @Override
  public DriverExecutionProfile getExecutionProfileForBoundStatements() {
    return statement.getExecutionProfile();
  }

  @Nullable
  @Override
  public CqlIdentifier getRoutingKeyspaceForBoundStatements() {
    return (statement.getKeyspace() != null)
        ? statement.getKeyspace()
        : statement.getRoutingKeyspace();
  }

  @Nullable
  @Override
  public ByteBuffer getRoutingKeyForBoundStatements() {
    return statement.getRoutingKey();
  }

  @Nullable
  @Override
  public Token getRoutingTokenForBoundStatements() {
    return statement.getRoutingToken();
  }

  @NonNull
  @Override
  public Map<String, ByteBuffer> getCustomPayloadForBoundStatements() {
    return statement.getCustomPayload();
  }

  @Nullable
  @Override
  public Boolean areBoundStatementsIdempotent() {
    return statement.isIdempotent();
  }

  @Nullable
  @Override
  public Duration getTimeoutForBoundStatements() {
    return statement.getTimeout();
  }

  @Nullable
  @Override
  public ByteBuffer getPagingStateForBoundStatements() {
    return statement.getPagingState();
  }

  @Override
  public int getPageSizeForBoundStatements() {
    return statement.getPageSize();
  }

  @Nullable
  @Override
  public ConsistencyLevel getConsistencyLevelForBoundStatements() {
    return statement.getConsistencyLevel();
  }

  @Nullable
  @Override
  public ConsistencyLevel getSerialConsistencyLevelForBoundStatements() {
    return statement.getSerialConsistencyLevel();
  }

  @Nullable
  @Override
  public Node getNode() {
    // never target prepare requests
    return null;
  }

  @Override
  public boolean areBoundStatementsTracing() {
    return statement.isTracing();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DefaultPrepareRequest) {
      DefaultPrepareRequest that = (DefaultPrepareRequest) other;
      return this.statement.equals(that.statement);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return statement.hashCode();
  }
}
