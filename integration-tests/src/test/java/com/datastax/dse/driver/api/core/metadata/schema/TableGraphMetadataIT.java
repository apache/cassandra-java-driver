/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRuleBuilder;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@DseRequirement(min = "6.8")
public class TableGraphMetadataIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<DseSession> SESSION_RULE =
      new DseSessionRuleBuilder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @BeforeClass
  public static void createTables() {
    DseSession session = SESSION_RULE.session();

    session.execute("CREATE TABLE person (name text PRIMARY KEY) " + "WITH VERTEX LABEL");
    session.execute(
        "CREATE TABLE software (company text, name text, version int, "
            + "PRIMARY KEY ((company, name), version)) "
            + "WITH VERTEX LABEL soft");
    session.execute(
        "CREATE TABLE contributors (contributor text, company_name text, software_name text, "
            + "software_version int, "
            + "PRIMARY KEY(contributor, company_name, software_name, software_version)) "
            + "WITH EDGE LABEL contrib "
            + "FROM person(contributor) "
            + "TO soft((company_name, software_name), software_version)");
  }

  @Test
  public void should_expose_vertex_and_edge_metadata() {
    DseSession session = SESSION_RULE.session();
    Metadata metadata = session.getMetadata();
    assertThat(metadata.getKeyspace(SESSION_RULE.keyspace()))
        .hasValueSatisfying(
            keyspaceMetadata -> {
              assertThat(keyspaceMetadata.getTable("person"))
                  .hasValueSatisfying(
                      person -> {
                        DseTableMetadata dsePerson = (DseTableMetadata) person;
                        assertThat(dsePerson.getVertex())
                            .hasValueSatisfying(
                                vertex ->
                                    assertThat(vertex.getLabelName())
                                        .isEqualTo(CqlIdentifier.fromInternal("person")));
                        assertThat(dsePerson.getEdge()).isEmpty();
                      });

              assertThat(keyspaceMetadata.getTable("software"))
                  .hasValueSatisfying(
                      software -> {
                        DseTableMetadata dseSoftware = (DseTableMetadata) software;
                        assertThat(dseSoftware.getVertex())
                            .hasValueSatisfying(
                                vertex ->
                                    assertThat(vertex.getLabelName())
                                        .isEqualTo(CqlIdentifier.fromInternal("soft")));
                        assertThat(dseSoftware.getEdge()).isEmpty();
                      });

              assertThat(keyspaceMetadata.getTable("contributors"))
                  .hasValueSatisfying(
                      contributors -> {
                        DseTableMetadata dseContributors = (DseTableMetadata) contributors;
                        assertThat(dseContributors.getVertex()).isEmpty();
                        assertThat(dseContributors.getEdge())
                            .hasValueSatisfying(
                                edge -> {
                                  assertThat(edge.getLabelName())
                                      .isEqualTo(CqlIdentifier.fromInternal("contrib"));

                                  assertThat(edge.getFromTable().asInternal()).isEqualTo("person");
                                  assertThat(edge.getFromLabel())
                                      .isEqualTo(CqlIdentifier.fromInternal("person"));
                                  assertThat(edge.getFromPartitionKeyColumns())
                                      .containsExactly(CqlIdentifier.fromInternal("contributor"));
                                  assertThat(edge.getFromClusteringColumns()).isEmpty();

                                  assertThat(edge.getToTable().asInternal()).isEqualTo("software");
                                  assertThat(edge.getToLabel())
                                      .isEqualTo(CqlIdentifier.fromInternal("soft"));
                                  assertThat(edge.getToPartitionKeyColumns())
                                      .containsExactly(
                                          CqlIdentifier.fromInternal("company_name"),
                                          CqlIdentifier.fromInternal("software_name"));
                                  assertThat(edge.getToClusteringColumns())
                                      .containsExactly(
                                          CqlIdentifier.fromInternal("software_version"));
                                });
                      });
            });
  }
}
