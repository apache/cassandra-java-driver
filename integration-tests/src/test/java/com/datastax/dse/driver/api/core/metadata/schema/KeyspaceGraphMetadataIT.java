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
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@DseRequirement(min = "6.8")
public class KeyspaceGraphMetadataIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<DseSession> SESSION_RULE =
      new DseSessionRuleBuilder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Test
  public void should_expose_graph_engine_if_set() {
    DseSession session = SESSION_RULE.session();
    session.execute(
        "CREATE KEYSPACE keyspace_metadata_it_graph_engine "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} "
            + "AND graph_engine = 'Tinker'");
    Metadata metadata = session.getMetadata();
    assertThat(metadata.getKeyspace("keyspace_metadata_it_graph_engine"))
        .hasValueSatisfying(
            keyspaceMetadata ->
                assertThat(((DseKeyspaceMetadata) keyspaceMetadata).getGraphEngine())
                    .hasValue("Tinker"));
  }

  @Test
  public void should_expose_empty_graph_engine_if_not_set() {
    // The default keyspace created by CcmRule has no graph engine
    Metadata metadata = SESSION_RULE.session().getMetadata();
    assertThat(metadata.getKeyspace(SESSION_RULE.keyspace()))
        .hasValueSatisfying(
            keyspaceMetadata ->
                assertThat(((DseKeyspaceMetadata) keyspaceMetadata).getGraphEngine()).isEmpty());
  }
}
