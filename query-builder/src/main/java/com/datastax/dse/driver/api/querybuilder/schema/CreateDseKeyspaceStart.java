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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.querybuilder.schema.KeyspaceReplicationOptions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

public interface CreateDseKeyspaceStart extends KeyspaceReplicationOptions<CreateDseKeyspace> {
  /**
   * Adds IF NOT EXISTS to the create keyspace specification. This indicates that the keyspace
   * should not be created it already exists.
   */
  @NonNull
  CreateDseKeyspaceStart ifNotExists();

  /**
   * Adds 'replication' options. One should only use this when they have a custom replication
   * strategy, otherwise it is advisable to use {@link #withSimpleStrategy(int)} or {@link
   * #withNetworkTopologyStrategy(Map)}.
   */
  @NonNull
  @Override
  CreateDseKeyspace withReplicationOptions(@NonNull Map<String, Object> replicationOptions);

  /**
   * Adds SimpleStrategy replication options with the given replication factor.
   *
   * <p>Note that using this will overwrite any previous use of this method or {@link
   * #withNetworkTopologyStrategy(Map)}.
   */
  @NonNull
  @Override
  default CreateDseKeyspace withSimpleStrategy(int replicationFactor) {
    ImmutableMap<String, Object> replication =
        ImmutableMap.<String, Object>builder()
            .put("class", "SimpleStrategy")
            .put("replication_factor", replicationFactor)
            .build();

    return withReplicationOptions(replication);
  }

  /**
   * Adds NetworkTopologyStrategy replication options with the given data center replication
   * factors.
   *
   * <p>Note that using this will overwrite any previous use of this method or {@link
   * #withSimpleStrategy(int)}.
   *
   * @param replications Mapping of data center name to replication factor to use for that data
   *     center.
   */
  @NonNull
  @Override
  default CreateDseKeyspace withNetworkTopologyStrategy(
      @NonNull Map<String, Integer> replications) {
    ImmutableMap.Builder<String, Object> replicationBuilder =
        ImmutableMap.<String, Object>builder().put("class", "NetworkTopologyStrategy");

    for (Map.Entry<String, Integer> replication : replications.entrySet()) {
      replicationBuilder.put(replication.getKey(), replication.getValue());
    }

    return withReplicationOptions(replicationBuilder.build());
  }
}
