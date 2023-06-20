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
package com.datastax.oss.driver.api.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.cql.DefaultBoundStatement;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class StatementProfileTest {

  private static final DriverExecutionProfile PROFILE = mock(DriverExecutionProfile.class);
  private static final String NAME = "mockProfileName";

  @Test
  @UseDataProvider("statements")
  public void should_set_profile_and_name_on_statement(
      Statement<?> statement,
      Operation operation1,
      Operation operation2,
      String expectedName,
      DriverExecutionProfile expectedProfile) {

    statement = operation1.applyTo(statement);
    statement = operation2.applyTo(statement);

    assertThat(statement.getExecutionProfileName()).isEqualTo(expectedName);
    assertThat(statement.getExecutionProfile()).isEqualTo(expectedProfile);
  }

  @Test
  @UseDataProvider("builders")
  public void should_set_profile_and_name_on_builder(
      StatementBuilder<?, ?> builder,
      Operation operation1,
      Operation operation2,
      String expectedName,
      DriverExecutionProfile expectedProfile) {

    builder = operation1.applyTo(builder);
    builder = operation2.applyTo(builder);

    Statement<?> statement = builder.build();

    assertThat(statement.getExecutionProfileName()).isEqualTo(expectedName);
    assertThat(statement.getExecutionProfile()).isEqualTo(expectedProfile);
  }

  private static Object[][] scenarios() {
    return new Object[][] {
      // operation1, operation2, expectedName, expectedProfile

      // only one set:
      new Object[] {setProfile(PROFILE), noop(), null, PROFILE},
      new Object[] {setName(NAME), noop(), NAME, null},

      // last one wins:
      new Object[] {setProfile(PROFILE), setName(NAME), NAME, null},
      new Object[] {setName(NAME), setProfile(PROFILE), null, PROFILE},

      // null does not unset other:
      new Object[] {setProfile(PROFILE), setName(null), null, PROFILE},
      new Object[] {setName(NAME), setProfile(null), NAME, null},
    };
  }

  @DataProvider
  public static Object[][] statements() {
    SimpleStatement simpleStatement = SimpleStatement.newInstance("mock query");
    Object[][] statements =
        TestDataProviders.fromList(
            simpleStatement,
            newBoundStatement(),
            BatchStatement.newInstance(BatchType.LOGGED, simpleStatement));

    return TestDataProviders.combine(statements, scenarios());
  }

  @DataProvider
  public static Object[][] builders() {
    SimpleStatement simpleStatement = SimpleStatement.newInstance("mock query");
    Object[][] builders =
        TestDataProviders.fromList(
            SimpleStatement.builder(simpleStatement),
            new BoundStatementBuilder(newBoundStatement()),
            BatchStatement.builder(BatchType.LOGGED).addStatement(simpleStatement));

    return TestDataProviders.combine(builders, scenarios());
  }

  private interface Operation {

    Statement<?> applyTo(Statement<?> statement);

    StatementBuilder<?, ?> applyTo(StatementBuilder<?, ?> builder);
  }

  private static Operation setProfile(DriverExecutionProfile profile) {
    return new Operation() {
      @Override
      public Statement<?> applyTo(Statement<?> statement) {
        return statement.setExecutionProfile(profile);
      }

      @Override
      public StatementBuilder<?, ?> applyTo(StatementBuilder<?, ?> builder) {
        return builder.setExecutionProfile(profile);
      }
    };
  }

  private static Operation setName(String name) {
    return new Operation() {
      @Override
      public Statement<?> applyTo(Statement<?> statement) {
        return statement.setExecutionProfileName(name);
      }

      @Override
      public StatementBuilder<?, ?> applyTo(StatementBuilder<?, ?> builder) {
        return builder.setExecutionProfileName(name);
      }
    };
  }

  private static Operation noop() {
    return new Operation() {
      @Override
      public Statement<?> applyTo(Statement<?> statement) {
        return statement;
      }

      @Override
      public StatementBuilder<?, ?> applyTo(StatementBuilder<?, ?> builder) {
        return builder;
      }
    };
  }

  private static BoundStatement newBoundStatement() {
    // Mock the minimum state needed to create a DefaultBoundStatement that can also be used to
    // initialize a builder
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    ColumnDefinitions variableDefinitions = mock(ColumnDefinitions.class);
    when(preparedStatement.getVariableDefinitions()).thenReturn(variableDefinitions);
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        new ByteBuffer[0],
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        Statement.NO_DEFAULT_TIMESTAMP,
        null,
        5000,
        null,
        null,
        Duration.ZERO,
        null,
        null,
        null,
        Statement.NO_NOW_IN_SECONDS);
  }
}
