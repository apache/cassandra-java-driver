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
package com.datastax.oss.driver.api.core.tracker;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.timeout;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.tracker.RequestLogger;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@Category(ParallelizableTests.class)
@RunWith(MockitoJUnitRunner.class)
public class RequestLoggerIT {

  private static final String QUERY = "SELECT release_version FROM system.local";

  @Rule
  public SimulacronRule simulacronRule = new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Rule
  public SessionRule<CqlSession> sessionRule =
      SessionRule.builder(simulacronRule)
          .withOptions(
              "request.tracker.class = com.datastax.oss.driver.internal.core.tracker.RequestLogger",
              "request.tracker.logs.success.enabled = true",
              "request.tracker.logs.slow.enabled = true",
              "request.tracker.logs.error.enabled = true",
              "request.tracker.logs.max-query-length = 500",
              "request.tracker.logs.show-values = true",
              "request.tracker.logs.max-value-length = 50",
              "request.tracker.logs.max-values = 50",
              "request.tracker.logs.show-stack-traces = true",
              "profiles.low-threshold.request.tracker.logs.slow.threshold = 1 nanosecond",
              "profiles.no-logs.request.tracker.logs.success.enabled = true",
              "profiles.no-logs.request.tracker.logs.slow.enabled = true",
              "profiles.no-logs.request.tracker.logs.error.enabled = true",
              "profiles.no-traces.request.tracker.logs.show-stack-traces = false")
          .build();

  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;
  @Mock private Appender<ILoggingEvent> appender;
  private Logger logger;
  private Level oldLevel;

  @Before
  public void setup() {
    logger = (Logger) LoggerFactory.getLogger(RequestLogger.class);
    oldLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    logger.addAppender(appender);
  }

  @After
  public void teardown() {
    logger.detachAppender(appender);
    logger.setLevel(oldLevel);
  }

  @Test
  public void should_log_successful_request() {
    // Given
    simulacronRule.cluster().prime(when(QUERY).then(rows().row("release_version", "3.0.0")));

    // When
    sessionRule.session().execute(QUERY);

    // Then
    Mockito.verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .contains("Success", "[0 values]", QUERY);
  }

  @Test
  public void should_log_failed_request_with_stack_trace() {
    // Given
    simulacronRule.cluster().prime(when(QUERY).then(serverError("test")));

    // When
    try {
      sessionRule.session().execute(QUERY);
      fail("Expected a ServerError");
    } catch (ServerError error) {
      // expected
    }

    // Then
    Mockito.verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    ILoggingEvent log = loggingEventCaptor.getValue();
    assertThat(log.getFormattedMessage())
        .contains("Error", "[0 values]", QUERY)
        .doesNotContain(ServerError.class.getName());
    assertThat(log.getThrowableProxy().getClassName()).isEqualTo(ServerError.class.getName());
  }

  @Test
  public void should_log_failed_request_without_stack_trace() {
    // Given
    simulacronRule.cluster().prime(when(QUERY).then(serverError("test")));

    // When
    try {
      sessionRule
          .session()
          .execute(SimpleStatement.builder(QUERY).withConfigProfileName("no-traces").build());
      fail("Expected a ServerError");
    } catch (ServerError error) {
      // expected
    }

    // Then
    Mockito.verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    ILoggingEvent log = loggingEventCaptor.getValue();
    assertThat(log.getFormattedMessage())
        .contains("Error", "[0 values]", QUERY, ServerError.class.getName());
    assertThat(log.getThrowableProxy()).isNull();
  }

  @Test
  public void should_log_slow_request() {
    // Given
    simulacronRule.cluster().prime(when(QUERY).then(rows().row("release_version", "3.0.0")));

    // When
    sessionRule
        .session()
        .execute(SimpleStatement.builder(QUERY).withConfigProfileName("low-threshold").build());

    // Then
    Mockito.verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .contains("Slow", "[0 values]", QUERY);
  }

  @Test
  public void should_not_log_when_disabled() {
    // Given
    simulacronRule.cluster().prime(when(QUERY).then(rows().row("release_version", "3.0.0")));

    // When
    sessionRule
        .session()
        .execute(SimpleStatement.builder(QUERY).withConfigProfileName("no-logs").build());
    // We expect no messages but logging is asynchronous, so add an extra message to make sure we've
    // waited long enough.
    logger.info("test");

    // Then
    Mockito.verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage()).isEqualTo("test");
  }
}
