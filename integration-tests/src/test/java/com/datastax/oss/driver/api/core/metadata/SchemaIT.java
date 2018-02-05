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
package com.datastax.oss.driver.api.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class SchemaIT {

  @Rule public CcmRule ccmRule = CcmRule.getInstance();

  @Rule public SessionRule<CqlSession> sessionRule = new SessionRule<>(ccmRule);

  @Test
  public void should_expose_system_and_test_keyspace() {
    Map<CqlIdentifier, KeyspaceMetadata> keyspaces =
        sessionRule.session().getMetadata().getKeyspaces();
    assertThat(keyspaces)
        .containsKeys(
            // Don't test exhaustively because system keyspaces depend on the Cassandra version, and
            // keyspaces from other tests might also be present
            CqlIdentifier.fromInternal("system"),
            CqlIdentifier.fromInternal("system_traces"),
            sessionRule.keyspace());
    assertThat(keyspaces.get(CqlIdentifier.fromInternal("system")).getTables())
        .containsKeys(CqlIdentifier.fromInternal("local"), CqlIdentifier.fromInternal("peers"));
  }

  @Test
  public void should_filter_by_keyspaces() {
    try (CqlSession session =
        SessionUtils.newSession(
            ccmRule,
            String.format(
                "metadata.schema.refreshed-keyspaces = [ \"%s\"] ",
                sessionRule.keyspace().asInternal()))) {

      assertThat(session.getMetadata().getKeyspaces()).containsOnlyKeys(sessionRule.keyspace());

      CqlIdentifier otherKeyspace = SessionUtils.uniqueKeyspaceId();
      SessionUtils.createKeyspace(session, otherKeyspace);

      assertThat(session.getMetadata().getKeyspaces()).containsOnlyKeys(sessionRule.keyspace());
    }
  }

  @Test
  public void should_not_load_schema_if_disabled_in_config() {
    try (CqlSession session = SessionUtils.newSession(ccmRule, "metadata.schema.enabled = false")) {

      assertThat(session.isSchemaMetadataEnabled()).isFalse();
      assertThat(session.getMetadata().getKeyspaces()).isEmpty();
    }
  }

  @Test
  public void should_enable_schema_programmatically_when_disabled_in_config() {
    try (CqlSession session = SessionUtils.newSession(ccmRule, "metadata.schema.enabled = false")) {

      assertThat(session.isSchemaMetadataEnabled()).isFalse();
      assertThat(session.getMetadata().getKeyspaces()).isEmpty();

      session.setSchemaMetadataEnabled(true);
      assertThat(session.isSchemaMetadataEnabled()).isTrue();

      ConditionChecker.checkThat(
              () -> assertThat(session.getMetadata().getKeyspaces()).isNotEmpty())
          .becomesTrue();
      assertThat(session.getMetadata().getKeyspaces())
          .containsKeys(
              CqlIdentifier.fromInternal("system"),
              CqlIdentifier.fromInternal("system_traces"),
              sessionRule.keyspace());

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
    DriverConfigProfile slowProfile = SessionUtils.slowProfile(session);
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE foo(k int primary key)")
                .withConfigProfile(slowProfile)
                .build());
    assertThat(session.getMetadata().getKeyspace(sessionRule.keyspace()).getTables())
        .doesNotContainKey(CqlIdentifier.fromInternal("foo"));

    // Reset to config value (true), should refresh and load the new table
    session.setSchemaMetadataEnabled(null);
    assertThat(session.isSchemaMetadataEnabled()).isTrue();
    ConditionChecker.checkThat(
            () ->
                assertThat(session.getMetadata().getKeyspace(sessionRule.keyspace()).getTables())
                    .containsKey(CqlIdentifier.fromInternal("foo")))
        .becomesTrue();
  }

  @Test
  public void should_refresh_schema_manually() {
    try (CqlSession session = SessionUtils.newSession(ccmRule, "metadata.schema.enabled = false")) {

      assertThat(session.isSchemaMetadataEnabled()).isFalse();
      assertThat(session.getMetadata().getKeyspaces()).isEmpty();

      Metadata newMetadata = session.refreshSchema();
      assertThat(newMetadata.getKeyspaces())
          .containsKeys(
              CqlIdentifier.fromInternal("system"),
              CqlIdentifier.fromInternal("system_traces"),
              sessionRule.keyspace());

      assertThat(session.getMetadata()).isSameAs(newMetadata);
    }
  }
}
