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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

class Cassandra22SchemaQueries extends SchemaQueries {
  Cassandra22SchemaQueries(
      DriverChannel channel,
      CompletableFuture<Metadata> refreshFuture,
      DriverConfigProfile config,
      String logPrefix) {
    super(channel, false, refreshFuture, config, logPrefix);
  }

  @Override
  protected String selectKeyspacesQuery() {
    return "SELECT * FROM system.schema_keyspaces";
  }

  @Override
  protected String selectTablesQuery() {
    return "SELECT * FROM system.schema_columnfamilies";
  }

  @Override
  protected Optional<String> selectViewsQuery() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> selectIndexesQuery() {
    return Optional.empty();
  }

  @Override
  protected String selectColumnsQuery() {
    return "SELECT * FROM system.schema_columns";
  }

  @Override
  protected String selectTypesQuery() {
    return "SELECT * FROM system.schema_usertypes";
  }

  @Override
  protected Optional<String> selectFunctionsQuery() {
    return Optional.of("SELECT * FROM system.schema_functions");
  }

  @Override
  protected Optional<String> selectAggregatesQuery() {
    return Optional.of("SELECT * FROM system.schema_aggregates");
  }
}
