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
package com.datastax.dse.driver.api.core.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.CqlSessionRuleBuilder;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@BackendRequirement(type = BackendType.DSE, minInclusive = "6.8")
public class KeyspaceGraphMetadataIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      new CqlSessionRuleBuilder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Test
  public void should_expose_graph_engine_if_set() {
    CqlSession session = SESSION_RULE.session();
    session.execute(
        "CREATE KEYSPACE keyspace_metadata_it_graph_engine "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} "
            + "AND graph_engine = 'Core'");
    Metadata metadata = session.getMetadata();
    assertThat(metadata.getKeyspace("keyspace_metadata_it_graph_engine"))
        .hasValueSatisfying(
            keyspaceMetadata ->
                assertThat(((DseGraphKeyspaceMetadata) keyspaceMetadata).getGraphEngine())
                    .hasValue("Core"));
  }

  @Test
  public void should_expose_graph_engine_if_keyspace_altered() {
    CqlSession session = SESSION_RULE.session();
    session.execute(
        "CREATE KEYSPACE keyspace_metadata_it_graph_engine_alter "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    assertThat(session.getMetadata().getKeyspace("keyspace_metadata_it_graph_engine_alter"))
        .hasValueSatisfying(
            keyspaceMetadata ->
                assertThat(((DseGraphKeyspaceMetadata) keyspaceMetadata).getGraphEngine())
                    .isEmpty());

    session.execute(
        "ALTER KEYSPACE keyspace_metadata_it_graph_engine_alter WITH graph_engine = 'Core'");
    assertThat(session.getMetadata().getKeyspace("keyspace_metadata_it_graph_engine_alter"))
        .hasValueSatisfying(
            keyspaceMetadata ->
                assertThat(((DseGraphKeyspaceMetadata) keyspaceMetadata).getGraphEngine())
                    .hasValue("Core"));
  }

  @Test
  public void should_not_allow_classic_graph_engine_to_be_specified_on_keyspace() {
    CqlSession session = SESSION_RULE.session();
    assertThatThrownBy(
            () ->
                session.execute(
                    "CREATE KEYSPACE keyspace_metadata_it_graph_engine_classic "
                        + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} "
                        + "AND graph_engine = 'Classic'"))
        .hasMessageContaining("Invalid/unknown graph engine name 'Classic'");
  }

  @Test
  public void should_expose_core_graph_engine_if_set() {
    CqlSession session = SESSION_RULE.session();
    session.execute(
        "CREATE KEYSPACE keyspace_metadata_it_graph_engine_core "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} "
            + "AND graph_engine = 'Core'");
    Metadata metadata = session.getMetadata();
    assertThat(metadata.getKeyspace("keyspace_metadata_it_graph_engine_core"))
        .hasValueSatisfying(
            keyspaceMetadata ->
                assertThat(((DseGraphKeyspaceMetadata) keyspaceMetadata).getGraphEngine())
                    .hasValue("Core"));
  }

  @Test
  public void should_expose_empty_graph_engine_if_not_set() {
    // The default keyspace created by CcmRule has no graph engine
    Metadata metadata = SESSION_RULE.session().getMetadata();
    assertThat(metadata.getKeyspace(SESSION_RULE.keyspace()))
        .hasValueSatisfying(
            keyspaceMetadata ->
                assertThat(((DseGraphKeyspaceMetadata) keyspaceMetadata).getGraphEngine())
                    .isEmpty());
  }
}
