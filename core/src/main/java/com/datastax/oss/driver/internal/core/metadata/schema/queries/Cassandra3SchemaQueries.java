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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import java.util.Optional;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class Cassandra3SchemaQueries extends CassandraSchemaQueries {
  public Cassandra3SchemaQueries(
      DriverChannel channel, Node node, DriverExecutionProfile config, String logPrefix) {
    super(channel, node, config, logPrefix);
  }

  @Override
  protected String selectKeyspacesQuery() {
    return "SELECT * FROM system_schema.keyspaces";
  }

  @Override
  protected String selectTablesQuery() {
    return "SELECT * FROM system_schema.tables";
  }

  @Override
  protected Optional<String> selectViewsQuery() {
    return Optional.of("SELECT * FROM system_schema.views");
  }

  @Override
  protected Optional<String> selectIndexesQuery() {
    return Optional.of("SELECT * FROM system_schema.indexes");
  }

  @Override
  protected String selectColumnsQuery() {
    return "SELECT * FROM system_schema.columns";
  }

  @Override
  protected String selectTypesQuery() {
    return "SELECT * FROM system_schema.types";
  }

  @Override
  protected Optional<String> selectFunctionsQuery() {
    return Optional.of("SELECT * FROM system_schema.functions");
  }

  @Override
  protected Optional<String> selectAggregatesQuery() {
    return Optional.of("SELECT * FROM system_schema.aggregates");
  }

  @Override
  protected Optional<String> selectVirtualKeyspacesQuery() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> selectVirtualTablesQuery() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> selectVirtualColumnsQuery() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> selectEdgesQuery() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> selectVerticiesQuery() {
    return Optional.empty();
  }
}
