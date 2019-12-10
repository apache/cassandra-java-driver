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
package com.datastax.oss.driver.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.time.Duration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Checks that case-sensitive UDT names are properly handled in schema metadata.
 *
 * <p>In Cassandra >= 2.2, whenever a UDT is referenced in a system table (e.g. {@code
 * system_schema.columns.type}, it uses the CQL form. This is in contrast to the UDT definition
 * itself ({@code system_schema.types.type_name}), which uses the internal form.
 *
 * @see <a href="https://datastax-oss.atlassian.net/browse/JAVA-2028">JAVA-2028</a>
 */
@Category(ParallelizableTests.class)
public class CaseSensitiveUdtIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Test
  public void should_expose_metadata_with_correct_case() {
    boolean supportsFunctions = CCM_RULE.getCassandraVersion().compareTo(Version.V2_2_0) >= 0;

    CqlSession session = SESSION_RULE.session();

    session.execute("CREATE TYPE \"Address\"(street text)");

    session.execute("CREATE TABLE user(id uuid PRIMARY KEY, address frozen<\"Address\">)");
    session.execute("CREATE TYPE t(a frozen<\"Address\">)");
    if (supportsFunctions) {
      session.execute(
          "CREATE FUNCTION eq(a \"Address\") "
              + "CALLED ON NULL INPUT "
              + "RETURNS \"Address\" "
              + "LANGUAGE java "
              + "AS $$return a;$$");
      session.execute(
          "CREATE FUNCTION left(l \"Address\", r \"Address\") "
              + "CALLED ON NULL INPUT "
              + "RETURNS \"Address\" "
              + "LANGUAGE java "
              + "AS $$return l;$$");
      session.execute(
          "CREATE AGGREGATE ag(\"Address\") "
              + "SFUNC left "
              + "STYPE \"Address\" "
              + "INITCOND {street: 'foo'};");
    }

    KeyspaceMetadata keyspace =
        session
            .getMetadata()
            .getKeyspace(SESSION_RULE.keyspace())
            .orElseThrow(() -> new AssertionError("Couldn't find rule's keyspace"));

    UserDefinedType addressType =
        keyspace
            .getUserDefinedType(CqlIdentifier.fromInternal("Address"))
            .orElseThrow(() -> new AssertionError("Couldn't find UDT definition"));

    assertThat(keyspace.getTable("user"))
        .hasValueSatisfying(
            table ->
                assertThat(table.getColumn("address"))
                    .hasValueSatisfying(
                        column -> assertThat(column.getType()).isEqualTo(addressType)));

    assertThat(keyspace.getUserDefinedType("t"))
        .hasValueSatisfying(type -> assertThat(type.getFieldTypes()).containsExactly(addressType));

    if (supportsFunctions) {
      assertThat(keyspace.getFunction("eq", addressType))
          .hasValueSatisfying(
              function -> {
                assertThat(function.getSignature().getParameterTypes())
                    .containsExactly(addressType);
                assertThat(function.getReturnType()).isEqualTo(addressType);
              });

      assertThat(keyspace.getAggregate("ag", addressType))
          .hasValueSatisfying(
              aggregate -> {
                assertThat(aggregate.getSignature().getParameterTypes())
                    .containsExactly(addressType);
                assertThat(aggregate.getStateType()).isEqualTo(addressType);
                assertThat(aggregate.getReturnType()).isEqualTo(addressType);
              });
    }
  }
}
