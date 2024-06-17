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
package com.datastax.dse.driver.api.core.graph;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
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

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "6.8.0",
    description = "DSE 6.8 required for graph paging")
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

    try (CqlSession session =
        CqlSession.builder()
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
