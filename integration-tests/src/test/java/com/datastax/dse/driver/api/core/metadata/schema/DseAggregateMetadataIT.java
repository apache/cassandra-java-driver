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
package com.datastax.dse.driver.api.core.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import java.util.Objects;
import java.util.Optional;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "5.0", description = "DSE 5.0+ required function/aggregate support")
public class DseAggregateMetadataIT extends AbstractMetadataIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static final Version DSE_6_0_0 = Objects.requireNonNull(Version.parse("6.0.0"));

  @Override
  protected SessionRule<CqlSession> getSessionRule() {
    return DseAggregateMetadataIT.SESSION_RULE;
  }

  @Test
  public void should_parse_aggregate_without_deterministic() {
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
              if (isDse6OrHigher()) {
                assertThat(aggregate.getDeterministic()).contains(false);
              } else {
                assertThat(aggregate.getDeterministic()).isEmpty();
              }
              assertThat(aggregate.getStateType()).isEqualTo(DataTypes.INT);
              assertThat(aggregate.describe(false))
                  .isEqualTo(
                      String.format(
                          "CREATE AGGREGATE \"%s\".\"nondeta\"() SFUNC \"nondetf\" STYPE int INITCOND 0;",
                          keyspace.getName().asInternal()));
            });
  }

  @Test
  public void should_parse_aggregate_with_deterministic() {
    assumeThat(isDse6OrHigher()).describedAs("DSE 6.0+ required for DETERMINISTIC").isTrue();
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
              assertThat(aggregate.getDeterministic()).contains(true);
              assertThat(aggregate.getStateType()).isEqualTo(DataTypes.INT);
              assertThat(aggregate.describe(false))
                  .isEqualTo(
                      String.format(
                          "CREATE AGGREGATE \"%s\".\"deta\"(int) SFUNC \"detf\" STYPE int INITCOND 0 DETERMINISTIC;",
                          keyspace.getName().asInternal()));
            });
  }

  private static boolean isDse6OrHigher() {
    assumeThat(CCM_RULE.getDseVersion())
        .describedAs("DSE required for DseFunctionMetadata tests")
        .isPresent();
    return CCM_RULE.getDseVersion().get().compareTo(DSE_6_0_0) >= 0;
  }
}
