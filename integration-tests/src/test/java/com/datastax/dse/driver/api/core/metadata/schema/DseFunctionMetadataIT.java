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
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.dse.driver.api.core.metadata.schema.DseFunctionMetadata.Monotonicity;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import java.util.Objects;
import java.util.Optional;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "5.0",
    description = "DSE 5.0+ required function/aggregate support")
public class DseFunctionMetadataIT extends AbstractMetadataIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static final Version DSE_6_0_0 = Objects.requireNonNull(Version.parse("6.0.0"));

  @Override
  public SessionRule<CqlSession> getSessionRule() {
    return DseFunctionMetadataIT.SESSION_RULE;
  }

  @Test
  public void should_parse_function_without_deterministic_or_monotonic() {
    String cqlFunction =
        "CREATE FUNCTION nondetf(i int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return new java.util.Random().nextInt(i);';";
    execute(cqlFunction);
    DseKeyspaceMetadata keyspace = getKeyspace();
    Optional<FunctionMetadata> functionOpt = keyspace.getFunction("nondetf", DataTypes.INT);
    assertThat(functionOpt.map(DseFunctionMetadata.class::cast))
        .hasValueSatisfying(
            function -> {
              if (isDse6OrHigher()) {
                assertThat(function.getDeterministic()).contains(false);
                assertThat(function.getMonotonicity()).contains(Monotonicity.NOT_MONOTONIC);
              } else {
                assertThat(function.getDeterministic()).isEmpty();
                assertThat(function.getMonotonicity()).isEmpty();
              }
              assertThat(function.getMonotonicArgumentNames()).isEmpty();
              assertThat(function.getLanguage()).isEqualTo("java");
              assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
              assertThat(function.getBody()).isEqualTo("return new java.util.Random().nextInt(i);");
              assertThat(function.describe(false))
                  .isEqualTo(
                      String.format(
                          "CREATE FUNCTION \"%s\".\"nondetf\"(\"i\" int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return new java.util.Random().nextInt(i);';",
                          keyspace.getName().asInternal()));
            });
  }

  @Test
  public void should_parse_function_with_deterministic() {
    assumeThat(isDse6OrHigher())
        .describedAs("DSE 6.0+ required for DETERMINISTIC / MONOTONIC")
        .isTrue();
    String cqlFunction =
        "CREATE FUNCTION detf(i int, y int) RETURNS NULL ON NULL INPUT RETURNS int DETERMINISTIC LANGUAGE java AS 'return i+y;';";
    execute(cqlFunction);
    DseKeyspaceMetadata keyspace = getKeyspace();
    Optional<FunctionMetadata> functionOpt =
        keyspace.getFunction("detf", DataTypes.INT, DataTypes.INT);
    assertThat(functionOpt.map(DseFunctionMetadata.class::cast))
        .hasValueSatisfying(
            function -> {
              assertThat(function.getDeterministic()).contains(true);
              assertThat(function.getMonotonicity()).contains(Monotonicity.NOT_MONOTONIC);
              assertThat(function.getMonotonicArgumentNames()).isEmpty();
              assertThat(function.getLanguage()).isEqualTo("java");
              assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
              assertThat(function.getBody()).isEqualTo("return i+y;");
              assertThat(function.describe(false))
                  .isEqualTo(
                      String.format(
                          "CREATE FUNCTION \"%s\".\"detf\"(\"i\" int,\"y\" int) RETURNS NULL ON NULL INPUT RETURNS int DETERMINISTIC LANGUAGE java AS 'return i+y;';",
                          keyspace.getName().asInternal()));
            });
  }

  @Test
  public void should_parse_function_with_monotonic() {
    assumeThat(isDse6OrHigher())
        .describedAs("DSE 6.0+ required for DETERMINISTIC / MONOTONIC")
        .isTrue();
    String cqlFunction =
        "CREATE FUNCTION monotonic(dividend int, divisor int) CALLED ON NULL INPUT RETURNS int MONOTONIC LANGUAGE java AS 'return dividend / divisor;';";
    execute(cqlFunction);
    DseKeyspaceMetadata keyspace = getKeyspace();
    Optional<FunctionMetadata> functionOpt =
        keyspace.getFunction("monotonic", DataTypes.INT, DataTypes.INT);
    assertThat(functionOpt.map(DseFunctionMetadata.class::cast))
        .hasValueSatisfying(
            function -> {
              assertThat(function.getDeterministic()).contains(false);
              assertThat(function.getMonotonicity()).contains(Monotonicity.FULLY_MONOTONIC);
              assertThat(function.getMonotonicArgumentNames())
                  .containsExactly(
                      CqlIdentifier.fromCql("dividend"), CqlIdentifier.fromCql("divisor"));
              assertThat(function.getLanguage()).isEqualTo("java");
              assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
              assertThat(function.getBody()).isEqualTo("return dividend / divisor;");
              assertThat(function.describe(false))
                  .isEqualTo(
                      String.format(
                          "CREATE FUNCTION \"%s\".\"monotonic\"(\"dividend\" int,\"divisor\" int) CALLED ON NULL INPUT RETURNS int MONOTONIC LANGUAGE java AS 'return dividend / divisor;';",
                          keyspace.getName().asInternal()));
            });
  }

  @Test
  public void should_parse_function_with_monotonic_on() {
    assumeThat(isDse6OrHigher())
        .describedAs("DSE 6.0+ required for DETERMINISTIC / MONOTONIC")
        .isTrue();
    String cqlFunction =
        "CREATE FUNCTION monotonic_on(dividend int, divisor int) CALLED ON NULL INPUT RETURNS int MONOTONIC ON \"dividend\" LANGUAGE java AS 'return dividend / divisor;';";
    execute(cqlFunction);
    DseKeyspaceMetadata keyspace = getKeyspace();
    Optional<FunctionMetadata> functionOpt =
        keyspace.getFunction("monotonic_on", DataTypes.INT, DataTypes.INT);
    assertThat(functionOpt.map(DseFunctionMetadata.class::cast))
        .hasValueSatisfying(
            function -> {
              assertThat(function.getDeterministic()).contains(false);
              assertThat(function.getMonotonicity()).contains(Monotonicity.PARTIALLY_MONOTONIC);
              assertThat(function.getMonotonicArgumentNames())
                  .containsExactly(CqlIdentifier.fromCql("dividend"));
              assertThat(function.getLanguage()).isEqualTo("java");
              assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
              assertThat(function.getBody()).isEqualTo("return dividend / divisor;");
              assertThat(function.describe(false))
                  .isEqualTo(
                      String.format(
                          "CREATE FUNCTION \"%s\".\"monotonic_on\"(\"dividend\" int,\"divisor\" int) CALLED ON NULL INPUT RETURNS int MONOTONIC ON \"dividend\" LANGUAGE java AS 'return dividend / divisor;';",
                          keyspace.getName().asInternal()));
            });
  }

  @Test
  public void should_parse_function_with_deterministic_and_monotonic() {
    assumeThat(isDse6OrHigher())
        .describedAs("DSE 6.0+ required for DETERMINISTIC / MONOTONIC")
        .isTrue();
    String cqlFunction =
        "CREATE FUNCTION det_and_monotonic(dividend int, divisor int) CALLED ON NULL INPUT RETURNS int DETERMINISTIC MONOTONIC LANGUAGE java AS 'return dividend / divisor;';";
    execute(cqlFunction);
    DseKeyspaceMetadata keyspace = getKeyspace();
    Optional<FunctionMetadata> functionOpt =
        keyspace.getFunction("det_and_monotonic", DataTypes.INT, DataTypes.INT);
    assertThat(functionOpt.map(DseFunctionMetadata.class::cast))
        .hasValueSatisfying(
            function -> {
              assertThat(function.getDeterministic()).contains(true);
              assertThat(function.getMonotonicity()).contains(Monotonicity.FULLY_MONOTONIC);
              assertThat(function.getMonotonicArgumentNames())
                  .containsExactly(
                      CqlIdentifier.fromCql("dividend"), CqlIdentifier.fromCql("divisor"));
              assertThat(function.getLanguage()).isEqualTo("java");
              assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
              assertThat(function.getBody()).isEqualTo("return dividend / divisor;");
              assertThat(function.describe(false))
                  .isEqualTo(
                      String.format(
                          "CREATE FUNCTION \"%s\".\"det_and_monotonic\"(\"dividend\" int,\"divisor\" int) CALLED ON NULL INPUT RETURNS int DETERMINISTIC MONOTONIC LANGUAGE java AS 'return dividend / divisor;';",
                          keyspace.getName().asInternal()));
            });
  }

  @Test
  public void should_parse_function_with_deterministic_and_monotonic_on() {
    assumeThat(isDse6OrHigher())
        .describedAs("DSE 6.0+ required for DETERMINISTIC / MONOTONIC")
        .isTrue();
    String cqlFunction =
        "CREATE FUNCTION det_and_monotonic_on(dividend int, divisor int) CALLED ON NULL INPUT RETURNS int DETERMINISTIC MONOTONIC ON \"dividend\" LANGUAGE java AS 'return dividend / divisor;';";
    execute(cqlFunction);
    DseKeyspaceMetadata keyspace = getKeyspace();
    Optional<FunctionMetadata> functionOpt =
        keyspace.getFunction("det_and_monotonic_on", DataTypes.INT, DataTypes.INT);
    assertThat(functionOpt.map(DseFunctionMetadata.class::cast))
        .hasValueSatisfying(
            function -> {
              assertThat(function.getDeterministic()).contains(true);
              assertThat(function.getMonotonicity()).contains(Monotonicity.PARTIALLY_MONOTONIC);
              assertThat(function.getMonotonicArgumentNames())
                  .containsExactly(CqlIdentifier.fromCql("dividend"));
              assertThat(function.getLanguage()).isEqualTo("java");
              assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
              assertThat(function.getBody()).isEqualTo("return dividend / divisor;");
              assertThat(function.describe(false))
                  .isEqualTo(
                      String.format(
                          "CREATE FUNCTION \"%s\".\"det_and_monotonic_on\"(\"dividend\" int,\"divisor\" int) CALLED ON NULL INPUT RETURNS int DETERMINISTIC MONOTONIC ON \"dividend\" LANGUAGE java AS 'return dividend / divisor;';",
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
