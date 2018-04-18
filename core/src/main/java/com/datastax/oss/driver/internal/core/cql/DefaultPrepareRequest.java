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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import java.nio.ByteBuffer;
import java.util.Map;
import net.jcip.annotations.Immutable;

/**
 * Default implementation of a prepare request, which is built internally to handle calls such as
 * {@link CqlSession#prepare(String)} and {@link CqlSession#prepare(SimpleStatement)}.
 *
 * <p>When a {@link SimpleStatement} gets prepared, some of its fields are propagated automatically.
 * For simplicity, this implementation makes the following opinionated choices:
 *
 * <ul>
 *   <li>the prepare request:
 *       <ul>
 *         <li>will use the same configuration profile (or configuration profile name) as the {@code
 *             SimpleStatement};
 *         <li>will use the same custom payload as the {@code SimpleStatement};
 *       </ul>
 *   <li>any bound statement created from the prepared statement:
 *       <ul>
 *         <li>will use the same configuration profile (or configuration profile name) as the {@code
 *             SimpleStatement};
 *         <li>will use the same custom payload as the {@code SimpleStatement};
 *         <li>will be idempotent if and only if the {@code SimpleStatement} was idempotent.
 *       </ul>
 * </ul>
 *
 * <p>This should be appropriate for most use cases; however if you need something more exotic (for
 * example, preparing with one profile, but executing bound statements with another one), you can
 * either write your own {@code PrepareRequest} implementation, or set the options manually on every
 * bound statement.
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

  @Override
  public String getQuery() {
    return statement.getQuery();
  }

  @Override
  public String getConfigProfileName() {
    return statement.getConfigProfileName();
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return statement.getConfigProfile();
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return statement.getKeyspace();
  }

  @Override
  public CqlIdentifier getRoutingKeyspace() {
    // Prepare requests do not operate on a particular partition, token-aware routing doesn't apply.
    return null;
  }

  @Override
  public ByteBuffer getRoutingKey() {
    return null;
  }

  @Override
  public Token getRoutingToken() {
    return null;
  }

  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return statement.getCustomPayload();
  }

  @Override
  public String getConfigProfileNameForBoundStatements() {
    return statement.getConfigProfileName();
  }

  @Override
  public DriverConfigProfile getConfigProfileForBoundStatements() {
    return statement.getConfigProfile();
  }

  @Override
  public Map<String, ByteBuffer> getCustomPayloadForBoundStatements() {
    return statement.getCustomPayload();
  }

  @Override
  public Boolean areBoundStatementsIdempotent() {
    return statement.isIdempotent();
  }
}
