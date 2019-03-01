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
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.cql.CqlRequestHandler;
import com.datastax.oss.driver.internal.core.tracker.RequestLogger;
import com.google.common.base.Strings;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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

  private static final CustomCcmRule CCM = new CustomCcmRule.Builder().build();
  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 20)
                  .withInt(
                      DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
                      RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH)
                  .withProfile(
                      "log-disabled",
                      DefaultDriverConfigLoaderBuilder.profileBuilder()
                          .withString(DefaultDriverOption.REQUEST_LOG_WARNINGS, "false")
                          .build())
                  .build())
          .build();
  private static final String KEY = "test";

  @ClassRule public static final TestRule CCM_RULE = RuleChain.outerRule(CCM).around(SESSION_RULE);

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;
  private Logger logger;
  private Level originalLoggerLevel;

  @BeforeClass
  public static void setupSchema() {
    // table with simple primary key, single cell.
    SESSION_RULE
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE IF NOT EXISTS test (k int primary key, v text)")
                .setExecutionProfile(SESSION_RULE.slowProfile())
                .build());
    for (int i = 0; i < 100; i++) {
      SESSION_RULE
          .session()
          .execute(
              SimpleStatement.builder("INSERT INTO test (k, v) VALUES (?, ?)")
                  .addPositionalValues(KEY, i)
                  .build());
    }
  }

  @Before
  public void setupLogger() {
    // setup the log appender
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
  public void should_execute_query_and_log_server_side_warnings() {
    final String query = "SELECT count(*) FROM test;";
    Statement<?> st = SimpleStatement.builder(String.format(query)).build();
    ResultSet result = SESSION_RULE.session().execute(st);

    ExecutionInfo executionInfo = result.getExecutionInfo();
    assertThat(executionInfo).isNotNull();
    List<String> warnings = executionInfo.getWarnings();
    assertThat(warnings).isNotNull();
    assertThat(warnings).isNotEmpty();
    String warning = warnings.get(0);
    assertThat(warning).isNotNull();
    assertThat(warning).isEqualTo("Aggregation query used without partition key");
    // verify the log was generated
    verify(appender, after(500).times(1)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getMessage()).isNotNull();
    String logMessage = loggingEventCaptor.getValue().getFormattedMessage();
    assertThat(logMessage)
        .startsWith(
            "Query '[0 values] "
                + query
                + "' generated server side warning(s): Aggregation query used without partition key");
  }

  @Test
  public void should_execute_query_and_not_log_server_side_warnings() {
    final String query = "SELECT count(*) FROM test;";
    Statement<?> st =
        SimpleStatement.builder(String.format(query))
            .setExecutionProfileName("log-disabled")
            .build();
    ResultSet result = SESSION_RULE.session().execute(st);

    ExecutionInfo executionInfo = result.getExecutionInfo();
    assertThat(executionInfo).isNotNull();
    List<String> warnings = executionInfo.getWarnings();
    assertThat(warnings).isNotNull();
    assertThat(warnings).isNotEmpty();
    String warning = warnings.get(0);
    assertThat(warning).isNotNull();
    assertThat(warning).isEqualTo("Aggregation query used without partition key");
    // verify the log was NOT generated
    verify(appender, after(500).times(0)).doAppend(loggingEventCaptor.capture());
  }

  @Test
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
    Statement<?> st = SimpleStatement.builder(String.format(query)).build();
    ResultSet result = SESSION_RULE.session().execute(st);
    ExecutionInfo executionInfo = result.getExecutionInfo();
    assertThat(executionInfo).isNotNull();
    List<String> warnings = executionInfo.getWarnings();
    assertThat(warnings).isNotEmpty();
    // verify the log was generated
    verify(appender, after(500).times(1)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getMessage()).isNotNull();
    String logMessage = loggingEventCaptor.getValue().getFormattedMessage();
    assertThat(logMessage)
        .startsWith("Query '")
        // query will only be logged up to MAX_QUERY_LENGTH
        // characters
        .contains(query.substring(0, RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH))
        .contains("' generated server side warning(s): ")
        .contains(
            String.format(
                "Batch for [%s.test] is of size 5152, exceeding specified threshold of 5120 by 32.",
                SESSION_RULE.keyspace().asCql(true)));
  }
}
