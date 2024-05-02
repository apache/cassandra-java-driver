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
package com.datastax.dse.driver.internal.querybuilder.schema;

import com.datastax.dse.driver.api.querybuilder.schema.AlterDseKeyspace;
import com.datastax.dse.driver.api.querybuilder.schema.AlterDseKeyspaceStart;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.internal.querybuilder.schema.OptionsUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultAlterDseKeyspace implements AlterDseKeyspaceStart, AlterDseKeyspace {

  private final CqlIdentifier keyspaceName;
  private final ImmutableMap<String, Object> options;

  public DefaultAlterDseKeyspace(@NonNull CqlIdentifier keyspaceName) {
    this(keyspaceName, ImmutableMap.of());
  }

  public DefaultAlterDseKeyspace(
      @NonNull CqlIdentifier keyspaceName, @NonNull ImmutableMap<String, Object> options) {
    this.keyspaceName = keyspaceName;
    this.options = options;
  }

  @NonNull
  @Override
  public AlterDseKeyspace withDurableWrites(boolean durableWrites) {
    return withOption("durable_writes", durableWrites);
  }

  @NonNull
  @Override
  public AlterDseKeyspace withGraphEngine(String graphEngine) {
    return this.withOption("graph_engine", graphEngine);
  }

  @NonNull
  @Override
  public AlterDseKeyspace withReplicationOptions(@NonNull Map<String, Object> replicationOptions) {
    return withOption("replication", replicationOptions);
  }

  @NonNull
  @Override
  public AlterDseKeyspace withSimpleStrategy(int replicationFactor) {
    ImmutableMap<String, Object> replication =
        ImmutableMap.<String, Object>builder()
            .put("class", "SimpleStrategy")
            .put("replication_factor", replicationFactor)
            .build();

    return withReplicationOptions(replication);
  }

  @NonNull
  @Override
  public AlterDseKeyspace withNetworkTopologyStrategy(@NonNull Map<String, Integer> replications) {
    ImmutableMap.Builder<String, Object> replicationBuilder =
        ImmutableMap.<String, Object>builder().put("class", "NetworkTopologyStrategy");

    for (Map.Entry<String, Integer> replication : replications.entrySet()) {
      replicationBuilder.put(replication.getKey(), replication.getValue());
    }

    return withReplicationOptions(replicationBuilder.build());
  }

  @NonNull
  @Override
  public AlterDseKeyspace withOption(@NonNull String name, @NonNull Object value) {
    return new DefaultAlterDseKeyspace(
        keyspaceName, ImmutableCollections.append(options, name, value));
  }

  @NonNull
  @Override
  public String asCql() {
    return "ALTER KEYSPACE " + keyspaceName.asCql(true) + OptionsUtils.buildOptions(options, true);
  }

  @NonNull
  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  @NonNull
  public CqlIdentifier getKeyspace() {
    return keyspaceName;
  }

  @Override
  public String toString() {
    return asCql();
  }
}
