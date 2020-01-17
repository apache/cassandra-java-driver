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
public class Cassandra4SchemaQueries extends Cassandra3SchemaQueries {
  public Cassandra4SchemaQueries(
      DriverChannel channel, Node node, DriverExecutionProfile config, String logPrefix) {
    super(channel, node, config, logPrefix);
  }

  @Override
  protected Optional<String> selectVirtualKeyspacesQuery() {
    return Optional.of("SELECT * FROM system_virtual_schema.keyspaces");
  }

  @Override
  protected Optional<String> selectVirtualTablesQuery() {
    return Optional.of("SELECT * FROM system_virtual_schema.tables");
  }

  @Override
  protected Optional<String> selectVirtualColumnsQuery() {
    return Optional.of("SELECT * FROM system_virtual_schema.columns");
  }
}
