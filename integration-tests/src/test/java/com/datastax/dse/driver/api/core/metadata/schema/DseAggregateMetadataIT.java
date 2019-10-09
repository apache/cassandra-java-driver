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
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import java.util.Optional;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "6.0")
public class DseAggregateMetadataIT extends AbstractMetadataIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<DseSession> SESSION_RULE =
      new DseSessionRuleBuilder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Override
  protected SessionRule<DseSession> getSessionRule() {
    return DseAggregateMetadataIT.SESSION_RULE;
  }

  @Test
  public void should_parse_aggregate_without_deterministic() throws Exception {
    String cqlFunction =
        "CREATE FUNCTION nondetf(i int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return new java.util.Random().nextInt(i);';";
    String cqlAggregate = "CREATE AGGREGATE nondeta() SFUNC nondetf STYPE int INITCOND 0;";
    execute(cqlFunction);
    execute(cqlAggregate);
    DseKeyspaceMetadata keyspace = getKeyspace();
    Optional<AggregateMetadata> aggregateOpt = keyspace.getAggregate("nondeta");
    assertThat(aggregateOpt.map(DseAggregateMetadata.class::cast))
        .hasValueSatisfying(
            aggregate -> {
              assertThat(aggregate.isDeterministic()).isFalse();
              assertThat(aggregate.getStateType()).isEqualTo(DataTypes.INT);
              assertThat(aggregate.describe(false))
                  .isEqualTo(
                      String.format(
                          "CREATE AGGREGATE \"%s\".\"nondeta\"() SFUNC \"nondetf\" STYPE int INITCOND 0;",
                          keyspace.getName().asInternal()));
            });
  }

  @Test
  public void should_parse_aggregate_with_deterministic() throws Exception {
    String cqlFunction =
        "CREATE FUNCTION detf(i int, y int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return i+y;';";
    String cqlAggregate =
        "CREATE AGGREGATE deta(int) SFUNC detf STYPE int INITCOND 0 DETERMINISTIC;";
    execute(cqlFunction);
    execute(cqlAggregate);
    DseKeyspaceMetadata keyspace = getKeyspace();
    Optional<AggregateMetadata> aggregateOpt = keyspace.getAggregate("deta", DataTypes.INT);
    assertThat(aggregateOpt.map(DseAggregateMetadata.class::cast))
        .hasValueSatisfying(
            aggregate -> {
              assertThat(aggregate.isDeterministic()).isTrue();
              assertThat(aggregate.getStateType()).isEqualTo(DataTypes.INT);
              assertThat(aggregate.describe(false))
                  .isEqualTo(
                      String.format(
                          "CREATE AGGREGATE \"%s\".\"deta\"(int) SFUNC \"detf\" STYPE int INITCOND 0 DETERMINISTIC;",
                          keyspace.getName().asInternal()));
            });
  }
}
