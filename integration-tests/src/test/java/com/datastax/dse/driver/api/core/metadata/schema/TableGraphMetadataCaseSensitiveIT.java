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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.CqlSessionRuleBuilder;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * A regression test for a specific case of schema parsing for graphs built from tables containing
 * case-sensitive column names in its tables. See JAVA-2492 for more information.
 */
@Category(ParallelizableTests.class)
@BackendRequirement(type = BackendType.DSE, minInclusive = "6.8")
public class TableGraphMetadataCaseSensitiveIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      new CqlSessionRuleBuilder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @BeforeClass
  public static void createTables() {
    CqlSession session = SESSION_RULE.session();

    session.execute(
        "CREATE TABLE \"Person\" (\"Name\" varchar, \"Age\" int, PRIMARY KEY ((\"Name\"), \"Age\")) WITH VERTEX LABEL");
    session.execute(
        "CREATE TABLE \"Software\" (\"Name\" varchar, \"Complexity\" int, PRIMARY KEY ((\"Name\"), \"Complexity\")) WITH VERTEX LABEL");
    session.execute(
        "CREATE TABLE \"Created\""
            + " (\"PersonName\" varchar, \"SoftwareName\" varchar, \"PersonAge\" int, \"SoftwareComplexity\" int, weight int,"
            + " primary key ((\"PersonName\"), \"SoftwareName\", weight)) WITH EDGE LABEL\n"
            + " FROM \"Person\"((\"PersonName\"),\"PersonAge\")"
            + " TO \"Software\"((\"SoftwareName\"),\"SoftwareComplexity\");");
  }

  @Test
  public void should_expose_case_sensitive_edge_metadata() {
    CqlSession session = SESSION_RULE.session();
    Metadata metadata = session.getMetadata();
    assertThat(metadata.getKeyspace(SESSION_RULE.keyspace()))
        .hasValueSatisfying(
            keyspaceMetadata ->
                assertThat(keyspaceMetadata.getTable(CqlIdentifier.fromInternal("Created")))
                    .hasValueSatisfying(
                        created -> {
                          DseGraphTableMetadata dseCreated = (DseGraphTableMetadata) created;
                          assertThat(dseCreated.getEdge())
                              .hasValueSatisfying(
                                  edge -> {
                                    assertThat(edge.getFromPartitionKeyColumns())
                                        .isEqualTo(
                                            ImmutableList.of(
                                                CqlIdentifier.fromInternal("PersonName")));
                                    assertThat(edge.getToPartitionKeyColumns())
                                        .isEqualTo(
                                            ImmutableList.of(
                                                CqlIdentifier.fromInternal("SoftwareName")));
                                    assertThat(edge.getFromClusteringColumns())
                                        .isEqualTo(
                                            ImmutableList.of(
                                                CqlIdentifier.fromInternal("PersonAge")));
                                    assertThat(edge.getToClusteringColumns())
                                        .isEqualTo(
                                            ImmutableList.of(
                                                CqlIdentifier.fromInternal("SoftwareComplexity")));
                                  });
                        }));
  }
}
