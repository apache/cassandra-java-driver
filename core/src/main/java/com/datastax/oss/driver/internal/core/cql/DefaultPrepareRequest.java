/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * For simplicity, this default implementation makes opinionated choices about {@code
 * *ForBoundStatements} methods: these should be appropriate for most cases, but not all use cases
 * are supported (for example, preparing with one profile and having the bound statements use
 * another profile is not possible). For exotic use cases, subclass or write your own
 * implementation.
 */
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
  public String getKeyspace() {
    // Not implemented yet, waiting for CASSANDRA-10145 to land in a release
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
