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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaIT {

  @ClassRule public static CcmRule ccmRule = CcmRule.getInstance();

  @ClassRule public static ClusterRule clusterRule = new ClusterRule(ccmRule);

  @Test
  public void should_expose_system_and_test_keyspace() {
    Map<CqlIdentifier, KeyspaceMetadata> keyspaces =
        clusterRule.cluster().getMetadata().getKeyspaces();
    assertThat(keyspaces)
        .containsKeys(
            // Don't test exhaustively because system keyspaces depend on the Cassandra version, and
            // keyspaces from other tests might also be present
            CqlIdentifier.fromInternal("system"),
            CqlIdentifier.fromInternal("system_traces"),
            clusterRule.keyspace());
    assertThat(keyspaces.get(CqlIdentifier.fromInternal("system")).getTables())
        .containsKeys(CqlIdentifier.fromInternal("local"), CqlIdentifier.fromInternal("peers"));
  }

  @Test
  public void should_filter_by_keyspaces() {
    try (Cluster cluster =
        ClusterUtils.newCluster(
            ccmRule,
            String.format(
                "metadata.schema.refreshed-keyspaces = [ \"%s\"] ",
                clusterRule.keyspace().asInternal()))) {

      assertThat(cluster.getMetadata().getKeyspaces()).containsOnlyKeys(clusterRule.keyspace());

      CqlIdentifier otherKeyspace = ClusterUtils.uniqueKeyspaceId();
      ClusterUtils.createKeyspace(cluster, otherKeyspace);

      assertThat(cluster.getMetadata().getKeyspaces()).containsOnlyKeys(clusterRule.keyspace());
    }
  }

  @Test
  public void should_not_load_schema_if_disabled_in_config() {
    try (Cluster cluster = ClusterUtils.newCluster(ccmRule, "metadata.schema.enabled = false")) {

      assertThat(cluster.isSchemaMetadataEnabled()).isFalse();
      assertThat(cluster.getMetadata().getKeyspaces()).isEmpty();
    }
  }

  @Test
  public void should_enable_schema_programmatically_when_disabled_in_config() {
    try (Cluster cluster = ClusterUtils.newCluster(ccmRule, "metadata.schema.enabled = false")) {

      assertThat(cluster.isSchemaMetadataEnabled()).isFalse();
      assertThat(cluster.getMetadata().getKeyspaces()).isEmpty();

      cluster.setSchemaMetadataEnabled(true);
      assertThat(cluster.isSchemaMetadataEnabled()).isTrue();

      ConditionChecker.checkThat(
              () -> assertThat(cluster.getMetadata().getKeyspaces()).isNotEmpty())
          .becomesTrue();
      assertThat(cluster.getMetadata().getKeyspaces())
          .containsKeys(
              CqlIdentifier.fromInternal("system"),
              CqlIdentifier.fromInternal("system_traces"),
              clusterRule.keyspace());

      cluster.setSchemaMetadataEnabled(null);
      assertThat(cluster.isSchemaMetadataEnabled()).isFalse();
    }
  }

  @Test
  public void should_disable_schema_programmatically_when_enabled_in_config() {
    Cluster cluster = clusterRule.cluster();
    cluster.setSchemaMetadataEnabled(false);
    assertThat(cluster.isSchemaMetadataEnabled()).isFalse();

    // Create a table, metadata should not be updated
    DriverConfigProfile slowProfile = ClusterUtils.slowProfile(cluster);
    clusterRule
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE foo(k int primary key)")
                .withConfigProfile(slowProfile)
                .build());
    assertThat(cluster.getMetadata().getKeyspace(clusterRule.keyspace()).getTables())
        .doesNotContainKey(CqlIdentifier.fromInternal("foo"));

    // Reset to config value (true), should refresh and load the new table
    cluster.setSchemaMetadataEnabled(null);
    assertThat(cluster.isSchemaMetadataEnabled()).isTrue();
    ConditionChecker.checkThat(
            () ->
                assertThat(cluster.getMetadata().getKeyspace(clusterRule.keyspace()).getTables())
                    .containsKey(CqlIdentifier.fromInternal("foo")))
        .becomesTrue();
  }

  @Test
  public void should_refresh_schema_manually() {
    try (Cluster cluster = ClusterUtils.newCluster(ccmRule, "metadata.schema.enabled = false")) {

      assertThat(cluster.isSchemaMetadataEnabled()).isFalse();
      assertThat(cluster.getMetadata().getKeyspaces()).isEmpty();

      Metadata newMetadata = cluster.refreshSchema();
      assertThat(newMetadata.getKeyspaces())
          .containsKeys(
              CqlIdentifier.fromInternal("system"),
              CqlIdentifier.fromInternal("system_traces"),
              clusterRule.keyspace());

      assertThat(cluster.getMetadata()).isSameAs(newMetadata);
    }
  }
}
