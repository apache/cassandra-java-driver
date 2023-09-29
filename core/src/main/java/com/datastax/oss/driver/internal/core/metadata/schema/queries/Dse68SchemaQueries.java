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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import java.util.Optional;

/**
 * The system table queries to refresh the schema in DSE 6.8.
 *
 * <p>There are two additional tables for per-table graph metadata.
 */
public class Dse68SchemaQueries extends Cassandra4SchemaQueries {

  public Dse68SchemaQueries(
      DriverChannel channel, Node node, DriverExecutionProfile config, String logPrefix) {
    super(channel, node, config, logPrefix);
  }

  @Override
  protected Optional<String> selectEdgesQuery() {
    return Optional.of("SELECT * FROM system_schema.edges");
  }

  @Override
  protected Optional<String> selectVerticiesQuery() {
    return Optional.of("SELECT * FROM system_schema.vertices");
  }
}
