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

package com.datastax.oss.driver.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.cql.CqlRequestHandler;
import com.datastax.oss.driver.internal.core.tracker.RequestLogger;
import com.google.common.base.Strings;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class ExecutionInfoWarningsIT {

  private static final String KEY = "test";

  private CustomCcmRule ccmRule =
      new CustomCcmRule.Builder()
          // set the warn threshold to 5Kb (default is 64Kb in newer versions)
          .withCassandraConfiguration("batch_size_warn_threshold_in_kb", "5")
          .build();
  private SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 20)
                  .withInt(
                      DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
                      RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH)
                  .startProfile("log-disabled")
                  .withString(DefaultDriverOption.REQUEST_LOG_WARNINGS, "false")
                  .build())
          .build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;
  private Logger logger;
  private Level originalLoggerLevel;

  @Before
  public void createSchema() {
    // table with simple primary key, single cell.
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE IF NOT EXISTS test (k int primary key, v text)")
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
    for (int i = 0; i < 100; i++) {
      sessionRule
          .session()
          .execute(
              SimpleStatement.builder("INSERT INTO test (k, v) VALUES (?, ?)")
                  .addPositionalValues(KEY, i)
                  .build());
    }
  }

  @Before
  public void setupLogger() {
    logger = (Logger) LoggerFactory.getLogger(CqlRequestHandler.class);
    originalLoggerLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    logger.addAppender(appender);
  }

  @After
  public void cleanupLogger() {
    logger.setLevel(originalLoggerLevel);
    logger.detachAppender(appender);
  }

  @Test
  @CassandraRequirement(min = "3.0")
  public void should_execute_query_and_log_server_side_warnings() {
    final String query = "SELECT count(*) FROM test;";
    Statement<?> st = SimpleStatement.builder(query).build();
    ResultSet result = sessionRule.session().execute(st);

    ExecutionInfo executionInfo = result.getExecutionInfo();
    assertThat(executionInfo).isNotNull();
    List<String> warnings = executionInfo.getWarnings();
    assertThat(warnings).isNotEmpty();
    String warning = warnings.get(0);
    assertThat(warning).isEqualTo("Aggregation query used without partition key");
    // verify the log was generated
    verify(appender, timeout(500).times(1)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getMessage()).isNotNull();
    String logMessage = loggingEventCaptor.getValue().getFormattedMessage();
    assertThat(logMessage)
        .startsWith(
            "Query '[0 values] "
                + query
                + "' generated server side warning(s): Aggregation query used without partition key");
  }

  @Test
  @CassandraRequirement(min = "3.0")
  public void should_execute_query_and_not_log_server_side_warnings() {
    final String query = "SELECT count(*) FROM test;";
    Statement<?> st =
        SimpleStatement.builder(query).setExecutionProfileName("log-disabled").build();
    ResultSet result = sessionRule.session().execute(st);

    ExecutionInfo executionInfo = result.getExecutionInfo();
    assertThat(executionInfo).isNotNull();
    List<String> warnings = executionInfo.getWarnings();
    assertThat(warnings).isNotEmpty();
    String warning = warnings.get(0);
    assertThat(warning).isEqualTo("Aggregation query used without partition key");
    // verify the log was NOT generated
    verify(appender, timeout(500).times(0)).doAppend(loggingEventCaptor.capture());
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_expose_warnings_on_execution_info() {
    // the default batch size warn threshold is 5 * 1024 bytes, but after CASSANDRA-10876 there must
    // be multiple mutations in a batch to trigger this warning so the batch includes 2 different
    // inserts.
    final String query =
        String.format(
            "BEGIN UNLOGGED BATCH\n"
                + "INSERT INTO test (k, v) VALUES (1, '%s')\n"
                + "INSERT INTO test (k, v) VALUES (2, '%s')\n"
                + "APPLY BATCH",
            Strings.repeat("1", 2 * 1024), Strings.repeat("1", 3 * 1024));
    Statement<?> st = SimpleStatement.builder(query).build();
    ResultSet result = sessionRule.session().execute(st);
    ExecutionInfo executionInfo = result.getExecutionInfo();
    assertThat(executionInfo).isNotNull();
    List<String> warnings = executionInfo.getWarnings();
    assertThat(warnings).isNotEmpty();
    // verify the log was generated
    verify(appender, timeout(500).atLeast(1)).doAppend(loggingEventCaptor.capture());
    List<String> logMessages =
        loggingEventCaptor.getAllValues().stream()
            .map(ILoggingEvent::getFormattedMessage)
            .collect(Collectors.toList());
    assertThat(logMessages)
        .anySatisfy(
            logMessage ->
                assertThat(logMessage)
                    .startsWith("Query '")
                    // different versiosns of Cassandra produce slightly different formated logs
                    // the .contains() below verify the common bits
                    .contains(
                        query.substring(0, RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH))
                    .contains("' generated server side warning(s): ")
                    .contains("Batch")
                    .contains("for")
                    .contains(String.format("%s.test", sessionRule.keyspace().asCql(true)))
                    .contains("is of size")
                    .containsPattern("exceeding specified .*threshold"));
  }
}
