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
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.annotations.Test;

@CCMConfig(config = {"batch_size_warn_threshold_in_kb:5"})
@CassandraVersion("2.2.0")
public class WarningsTest extends CCMTestsSupport {

  @Override
  public void onTestContextInitialized() {
    execute("CREATE TABLE foo(k int primary key, v text)");
  }

  @Test(groups = "short")
  public void should_expose_warnings_on_execution_info() {
    // the default batch size warn threshold is 5 * 1024 bytes, but after CASSANDRA-10876 there must
    // be
    // multiple mutations in a batch to trigger this warning so the batch includes 2 different
    // inserts.
    ResultSet rs =
        session()
            .execute(
                String.format(
                    "BEGIN UNLOGGED BATCH\n"
                        + "INSERT INTO foo (k, v) VALUES (1, '%s')\n"
                        + "INSERT INTO foo (k, v) VALUES (2, '%s')\n"
                        + "APPLY BATCH",
                    Strings.repeat("1", 2 * 1024), Strings.repeat("1", 3 * 1024)));

    List<String> warnings = rs.getExecutionInfo().getWarnings();
    assertThat(warnings).hasSize(1);
  }

  @Test(groups = "short")
  public void should_execute_query_with_server_side_warnings() {
    // Set the system property to enable logging of server side warnings
    final String originalLoggingFlag =
        System.getProperty(RequestHandler.LOG_REQUEST_WARNINGS_PROPERTY, "false");
    System.setProperty(RequestHandler.LOG_REQUEST_WARNINGS_PROPERTY, "true");
    assertThat(Boolean.getBoolean(RequestHandler.LOG_REQUEST_WARNINGS_PROPERTY)).isTrue();
    // create a TestAppender and add it
    TestAppender logAppender = new TestAppender();
    Logger.getRootLogger().addAppender(logAppender);
    // Given a query that will produce server side warnings that will be embedded in the
    // ExecutionInfo
    SimpleStatement statement = new SimpleStatement("SELECT count(*) FROM foo");

    ResultSet rs = session().execute(statement);
    ExecutionInfo ei = rs.getExecutionInfo();
    // When
    Row row = rs.one();

    // Then
    assertThat(row).isNotNull();
    List<String> warnings = ei.getWarnings();

    assertThat(warnings).isNotEmpty();
    assertThat(warnings.size()).isEqualTo(1);
    assertThat(warnings.get(0)).isEqualTo("Aggregation query used without partition key");
    // assert the log was generated
    assertThat(logAppender).isNotNull();
    assertThat(logAppender.logs).isNotNull();
    assertThat(logAppender.logs).isNotEmpty();
    assertThat(logAppender.logs.get(0).getMessage())
        .isEqualTo("Aggregation query used without partition key");
    // remove the log appender
    Logger.getRootLogger().removeAppender(logAppender);
    // reset the logging flag
    System.setProperty(RequestHandler.LOG_REQUEST_WARNINGS_PROPERTY, originalLoggingFlag);
  }

  private class TestAppender extends WriterAppender {

    private ArrayList<LoggingEvent> logs = new ArrayList();

    @Override
    public void append(LoggingEvent event) {
      logs.add(event);
    }
  }
}
