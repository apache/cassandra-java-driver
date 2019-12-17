/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.specex.ConstantSpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.specex.NoSpeculativeExecutionPolicy;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.time.Duration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@DseRequirement(min = "6.8.0", description = "DSE 6.8 required for graph paging")
@RunWith(DataProviderRunner.class)
public class GraphSpeculativeExecutionIT {

  @ClassRule
  public static CustomCcmRule ccmRule = CustomCcmRule.builder().withDseWorkloads("graph").build();

  @Test
  @UseDataProvider("idempotenceAndSpecExecs")
  public void should_use_speculative_executions_when_enabled(
      boolean defaultIdempotence,
      Boolean statementIdempotence,
      Class<?> speculativeExecutionClass,
      boolean expectSpeculativeExecutions) {

    try (DseSession session =
        DseSession.builder()
            .addContactEndPoints(ccmRule.getContactPoints())
            .withConfigLoader(
                SessionUtils.configLoaderBuilder()
                    .withBoolean(
                        DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, defaultIdempotence)
                    .withInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, 10)
                    .withClass(
                        DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
                        speculativeExecutionClass)
                    .withDuration(
                        DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY, Duration.ofMillis(10))
                    .withString(DseDriverOption.GRAPH_PAGING_ENABLED, "ENABLED")
                    .build())
            .build()) {

      ScriptGraphStatement statement =
          ScriptGraphStatement.newInstance(
                  "java.util.concurrent.TimeUnit.MILLISECONDS.sleep(1000L);")
              .setIdempotent(statementIdempotence);

      GraphResultSet result = session.execute(statement);
      int speculativeExecutionCount =
          result.getRequestExecutionInfo().getSpeculativeExecutionCount();
      if (expectSpeculativeExecutions) {
        assertThat(speculativeExecutionCount).isGreaterThan(0);
      } else {
        assertThat(speculativeExecutionCount).isEqualTo(0);
      }
    }
  }

  @DataProvider
  public static Object[][] idempotenceAndSpecExecs() {
    return new Object[][] {
      new Object[] {false, false, NoSpeculativeExecutionPolicy.class, false},
      new Object[] {false, true, NoSpeculativeExecutionPolicy.class, false},
      new Object[] {false, null, NoSpeculativeExecutionPolicy.class, false},
      new Object[] {true, false, NoSpeculativeExecutionPolicy.class, false},
      new Object[] {true, true, NoSpeculativeExecutionPolicy.class, false},
      new Object[] {true, null, NoSpeculativeExecutionPolicy.class, false},
      new Object[] {false, false, ConstantSpeculativeExecutionPolicy.class, false},
      new Object[] {false, true, ConstantSpeculativeExecutionPolicy.class, true},
      new Object[] {false, null, ConstantSpeculativeExecutionPolicy.class, false},
      new Object[] {true, false, ConstantSpeculativeExecutionPolicy.class, false},
      new Object[] {true, true, ConstantSpeculativeExecutionPolicy.class, true},
      new Object[] {true, null, ConstantSpeculativeExecutionPolicy.class, true},
    };
  }
}
