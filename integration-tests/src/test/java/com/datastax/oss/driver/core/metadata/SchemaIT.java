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
package com.datastax.oss.driver.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.junit.AssumptionViolatedException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class SchemaIT {

  private static final Version DSE_MIN_VIRTUAL_TABLES =
      Objects.requireNonNull(Version.parse("6.7.0"));

  private final CcmRule ccmRule = CcmRule.getInstance();

  private final SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Test
  public void should_not_expose_system_and_test_keyspace() {
    Map<CqlIdentifier, KeyspaceMetadata> keyspaces =
        sessionRule.session().getMetadata().getKeyspaces();
    assertThat(keyspaces)
        .doesNotContainKeys(
            // Don't test exhaustively because system keyspaces depend on the Cassandra version, and
            // keyspaces from other tests might also be present
            CqlIdentifier.fromInternal("system"), CqlIdentifier.fromInternal("system_traces"));
  }

  @Test
  public void should_expose_test_keyspace() {
    Map<CqlIdentifier, KeyspaceMetadata> keyspaces =
        sessionRule.session().getMetadata().getKeyspaces();
    assertThat(keyspaces).containsKey(sessionRule.keyspace());
  }

  @Test
  public void should_filter_by_keyspaces() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(
                DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES,
                Collections.singletonList(sessionRule.keyspace().asInternal()))
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {

      assertThat(session.getMetadata().getKeyspaces()).containsOnlyKeys(sessionRule.keyspace());

      CqlIdentifier otherKeyspace = SessionUtils.uniqueKeyspaceId();
      SessionUtils.createKeyspace(session, otherKeyspace);

      assertThat(session.getMetadata().getKeyspaces()).containsOnlyKeys(sessionRule.keyspace());
    }
  }

  @Test
  public void should_not_load_schema_if_disabled_in_config() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, false)
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {

      assertThat(session.isSchemaMetadataEnabled()).isFalse();
      assertThat(session.getMetadata().getKeyspaces()).isEmpty();
    }
  }

  @Test
  public void should_enable_schema_programmatically_when_disabled_in_config() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, false)
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {

      assertThat(session.isSchemaMetadataEnabled()).isFalse();
      assertThat(session.getMetadata().getKeyspaces()).isEmpty();

      session.setSchemaMetadataEnabled(true);
      assertThat(session.isSchemaMetadataEnabled()).isTrue();

      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(session.getMetadata().getKeyspaces()).isNotEmpty());
      assertThat(session.getMetadata().getKeyspaces()).containsKey(sessionRule.keyspace());

      session.setSchemaMetadataEnabled(null);
      assertThat(session.isSchemaMetadataEnabled()).isFalse();
    }
  }

  @Test
  public void should_disable_schema_programmatically_when_enabled_in_config() {
    CqlSession session = sessionRule.session();
    session.setSchemaMetadataEnabled(false);
    assertThat(session.isSchemaMetadataEnabled()).isFalse();

    // Create a table, metadata should not be updated
    DriverExecutionProfile slowProfile = SessionUtils.slowProfile(session);
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE foo(k int primary key)")
                .setExecutionProfile(slowProfile)
                .build());
    assertThat(session.getMetadata().getKeyspace(sessionRule.keyspace()).get().getTables())
        .doesNotContainKey(CqlIdentifier.fromInternal("foo"));

    // Reset to config value (true), should refresh and load the new table
    session.setSchemaMetadataEnabled(null);
    assertThat(session.isSchemaMetadataEnabled()).isTrue();
    await()
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertThat(
                        session.getMetadata().getKeyspace(sessionRule.keyspace()).get().getTables())
                    .containsKey(CqlIdentifier.fromInternal("foo")));
  }

  @Test
  public void should_refresh_schema_manually() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, false)
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {

      assertThat(session.isSchemaMetadataEnabled()).isFalse();
      assertThat(session.getMetadata().getKeyspaces()).isEmpty();

      Metadata newMetadata = session.refreshSchema();
      assertThat(newMetadata.getKeyspaces()).containsKey(sessionRule.keyspace());

      assertThat(session.getMetadata()).isSameAs(newMetadata);
    }
  }

  @BackendRequirement(
      type = BackendType.CASSANDRA,
      minInclusive = "4.0",
      description = "virtual tables introduced in 4.0")
  @Test
  public void should_get_virtual_metadata() {
    skipIfDse60();

    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(
                DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES,
                Collections.singletonList("system_views"))
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {

      Metadata md = session.getMetadata();
      KeyspaceMetadata kmd = md.getKeyspace("system_views").get();

      // Keyspace name should be set, marked as virtual, and have at least sstable_tasks table.
      // All other values should be defaulted since they are not defined in the virtual schema
      // tables.
      assertThat(kmd.getTables().size()).isGreaterThanOrEqualTo(1);
      assertThat(kmd.isVirtual()).isTrue();
      assertThat(kmd.isDurableWrites()).isFalse();
      assertThat(kmd.getName().asCql(true)).isEqualTo("system_views");

      // Virtual tables lack User Types, Functions, Views and Aggregates
      assertThat(kmd.getUserDefinedTypes().size()).isEqualTo(0);
      assertThat(kmd.getFunctions().size()).isEqualTo(0);
      assertThat(kmd.getViews().size()).isEqualTo(0);
      assertThat(kmd.getAggregates().size()).isEqualTo(0);

      assertThat(kmd.describe(true))
          .isEqualTo(
              "/* VIRTUAL KEYSPACE system_views WITH replication = { 'class' : 'null' } "
                  + "AND durable_writes = false; */");
      // Table name should be set, marked as virtual, and it should have columns set.
      // indexes, views, clustering column, clustering order and id are not defined in the virtual
      // schema tables.
      TableMetadata tm = kmd.getTable("sstable_tasks").get();
      assertThat(tm).isNotNull();
      assertThat(tm.getName().toString()).isEqualTo("sstable_tasks");
      assertThat(tm.isVirtual()).isTrue();
      // DSE 6.8+ reports 7 columns, Cassandra 4+ reports 8 columns
      assertThat(tm.getColumns().size()).isGreaterThanOrEqualTo(7);
      assertThat(tm.getIndexes().size()).isEqualTo(0);
      assertThat(tm.getPartitionKey().size()).isEqualTo(1);
      assertThat(tm.getPartitionKey().get(0).getName().toString()).isEqualTo("keyspace_name");
      assertThat(tm.getClusteringColumns().size()).isEqualTo(2);
      assertThat(tm.getId().isPresent()).isFalse();
      assertThat(tm.getOptions().size()).isEqualTo(0);
      assertThat(tm.getKeyspace()).isEqualTo(kmd.getName());
      assertThat(tm.describe(true))
          .isIn(
              // DSE 6.8+
              "/* VIRTUAL TABLE system_views.sstable_tasks (\n"
                  + "    keyspace_name text,\n"
                  + "    table_name text,\n"
                  + "    task_id uuid,\n"
                  + "    kind text,\n"
                  + "    progress bigint,\n"
                  + "    total bigint,\n"
                  + "    unit text,\n"
                  + "    PRIMARY KEY (keyspace_name, table_name, task_id)\n"
                  + "); */",
              // Cassandra 4.0
              "/* VIRTUAL TABLE system_views.sstable_tasks (\n"
                  + "    keyspace_name text,\n"
                  + "    table_name text,\n"
                  + "    task_id uuid,\n"
                  + "    completion_ratio double,\n"
                  + "    kind text,\n"
                  + "    progress bigint,\n"
                  + "    total bigint,\n"
                  + "    unit text,\n"
                  + "    PRIMARY KEY (keyspace_name, table_name, task_id)\n"
                  + "); */",
              // Cassandra 4.1
              "/* VIRTUAL TABLE system_views.sstable_tasks (\n"
                  + "    keyspace_name text,\n"
                  + "    table_name text,\n"
                  + "    task_id timeuuid,\n"
                  + "    completion_ratio double,\n"
                  + "    kind text,\n"
                  + "    progress bigint,\n"
                  + "    sstables int,\n"
                  + "    total bigint,\n"
                  + "    unit text,\n"
                  + "    PRIMARY KEY (keyspace_name, table_name, task_id)\n"
                  + "); */",
              // Cassandra 5.0
              "/* VIRTUAL TABLE system_views.sstable_tasks (\n"
                  + "    keyspace_name text,\n"
                  + "    table_name text,\n"
                  + "    task_id timeuuid,\n"
                  + "    completion_ratio double,\n"
                  + "    kind text,\n"
                  + "    progress bigint,\n"
                  + "    sstables int,\n"
                  + "    target_directory text,\n"
                  + "    total bigint,\n"
                  + "    unit text,\n"
                  + "    PRIMARY KEY (keyspace_name, table_name, task_id)\n"
                  + "); */");
      // ColumnMetadata is as expected
      ColumnMetadata cm = tm.getColumn("progress").get();
      assertThat(cm).isNotNull();
      assertThat(cm.getParent()).isEqualTo(tm.getName());
      assertThat(cm.getType()).isEqualTo(DataTypes.BIGINT);
      assertThat(cm.getName().toString()).isEqualTo("progress");
    }
  }

  @BackendRequirement(
      type = BackendType.CASSANDRA,
      minInclusive = "4.0",
      description = "virtual tables introduced in 4.0")
  @Test
  public void should_exclude_virtual_keyspaces_from_token_map() {
    skipIfDse60();

    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(
                DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES,
                Arrays.asList(
                    "system_views", "system_virtual_schema", sessionRule.keyspace().asInternal()))
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {
      Metadata metadata = session.getMetadata();
      Map<CqlIdentifier, KeyspaceMetadata> keyspaces = metadata.getKeyspaces();
      assertThat(keyspaces)
          .containsKey(CqlIdentifier.fromCql("system_views"))
          .containsKey(CqlIdentifier.fromCql("system_virtual_schema"));

      TokenMap tokenMap = metadata.getTokenMap().orElseThrow(AssertionError::new);
      ByteBuffer partitionKey = Bytes.fromHexString("0x00"); // value does not matter
      assertThat(tokenMap.getReplicas("system_views", partitionKey)).isEmpty();
      assertThat(tokenMap.getReplicas("system_virtual_schema", partitionKey)).isEmpty();
      // Check that a non-virtual keyspace is present
      assertThat(tokenMap.getReplicas(sessionRule.keyspace(), partitionKey)).isNotEmpty();
    }
  }

  private void skipIfDse60() {
    // Special case: DSE 6.0 reports C* 4.0 but does not support virtual tables
    if (!ccmRule.isDistributionOf(
        BackendType.DSE, (dist, cass) -> dist.compareTo(DSE_MIN_VIRTUAL_TABLES) >= 0)) {
      throw new AssumptionViolatedException("DSE 6.0 does not support virtual tables");
    }
  }
}
