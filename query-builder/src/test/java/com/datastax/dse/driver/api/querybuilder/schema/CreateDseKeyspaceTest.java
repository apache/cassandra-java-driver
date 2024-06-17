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

import static com.datastax.dse.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.dse.driver.api.querybuilder.DseSchemaBuilder.createDseKeyspace;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import org.junit.Test;

public class CreateDseKeyspaceTest {

  @Test
  public void should_not_throw_on_toString_for_CreateKeyspaceStart() {
    assertThat(createDseKeyspace("foo").toString()).isEqualTo("CREATE KEYSPACE foo");
  }

  @Test
  public void should_generate_create_keyspace_simple_strategy() {
    assertThat(createDseKeyspace("foo").withSimpleStrategy(5))
        .hasCql(
            "CREATE KEYSPACE foo WITH replication={'class':'SimpleStrategy','replication_factor':5}");
  }

  @Test
  public void should_generate_create_keyspace_simple_strategy_and_durable_writes() {
    assertThat(createDseKeyspace("foo").withSimpleStrategy(5).withDurableWrites(true))
        .hasCql(
            "CREATE KEYSPACE foo WITH replication={'class':'SimpleStrategy','replication_factor':5} AND durable_writes=true");
  }

  @Test
  public void should_generate_create_keyspace_if_not_exists() {
    assertThat(createDseKeyspace("foo").ifNotExists().withSimpleStrategy(2))
        .hasCql(
            "CREATE KEYSPACE IF NOT EXISTS foo WITH replication={'class':'SimpleStrategy','replication_factor':2}");
  }

  @Test
  public void should_generate_create_keyspace_network_topology_strategy() {
    assertThat(
            createDseKeyspace("foo")
                .withNetworkTopologyStrategy(ImmutableMap.of("dc1", 3, "dc2", 4)))
        .hasCql(
            "CREATE KEYSPACE foo WITH replication={'class':'NetworkTopologyStrategy','dc1':3,'dc2':4}");
  }

  @Test
  public void should_generate_create_keyspace_with_graph_engine() {
    assertThat(
            createDseKeyspace("foo")
                .ifNotExists()
                .withNetworkTopologyStrategy(ImmutableMap.of("dc1", 3, "dc2", 4))
                .withDurableWrites(true)
                .withGraphEngine("Core"))
        .hasCql(
            "CREATE KEYSPACE IF NOT EXISTS foo "
                + "WITH replication={'class':'NetworkTopologyStrategy','dc1':3,'dc2':4} "
                + "AND durable_writes=true "
                + "AND graph_engine='Core'");
  }

  @Test
  public void should_generate_create_keyspace_with_custom_properties() {
    assertThat(
            createDseKeyspace("foo")
                .withSimpleStrategy(3)
                .withOption("awesome_feature", true)
                .withOption("wow_factor", 11)
                .withOption("random_string", "hi"))
        .hasCql(
            "CREATE KEYSPACE foo WITH replication={'class':'SimpleStrategy','replication_factor':3} AND awesome_feature=true AND wow_factor=11 AND random_string='hi'");
  }
}
