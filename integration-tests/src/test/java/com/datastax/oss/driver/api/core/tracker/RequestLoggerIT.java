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

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.tracker.RequestLogger;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
import org.mockito.internal.verification.VerificationModeFactory;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.verification.Timeout;
import org.slf4j.LoggerFactory;

@Category(ParallelizableTests.class)
@RunWith(MockitoJUnitRunner.class)
public class RequestLoggerIT {

  private static final String QUERY = "SELECT release_version FROM system.local";

  @Rule
  public SimulacronRule simulacronRule = new SimulacronRule(ClusterSpec.builder().withNodes(3));

  @Rule
  public SessionRule<CqlSession> sessionRuleRequest =
      SessionRule.builder(simulacronRule)
          .withOptions(
              "advanced.request-tracker.class = com.datastax.oss.driver.internal.core.tracker.RequestLogger",
              "advanced.request-tracker.logs.success.enabled = true",
              "advanced.request-tracker.logs.slow.enabled = true",
              "advanced.request-tracker.logs.error.enabled = true",
              "advanced.request-tracker.logs.max-query-length = 500",
              "advanced.request-tracker.logs.show-values = true",
              "advanced.request-tracker.logs.max-value-length = 50",
              "advanced.request-tracker.logs.max-values = 50",
              "advanced.request-tracker.logs.show-stack-traces = true",
              "advanced.request-tracker.logs.node-level = false",
              "profiles.low-threshold.advanced.request-tracker.logs.slow.threshold = 1 nanosecond",
              "profiles.no-logs.advanced.request-tracker.logs.success.enabled = false",
              "profiles.no-logs.advanced.request-tracker.logs.slow.enabled = false",
              "profiles.no-logs.advanced.request-tracker.logs.error.enabled = false",
              "profiles.no-traces.advanced.request-tracker.logs.show-stack-traces = false")
          .build();

  @Rule
  public SessionRule<CqlSession> sessionRuleNode =
      SessionRule.builder(simulacronRule)
          .withOptions(
              "basic.request.consistency = ONE",
              "advanced.request-tracker.class = com.datastax.oss.driver.api.core.tracker.RequestNodeLoggerExample",
              "advanced.request-tracker.logs.success.enabled = true",
              "advanced.request-tracker.logs.slow.enabled = true",
              "advanced.request-tracker.logs.error.enabled = true",
              "advanced.request-tracker.logs.max-query-length = 500",
              "advanced.request-tracker.logs.show-values = true",
              "advanced.request-tracker.logs.max-value-length = 50",
              "advanced.request-tracker.logs.max-values = 50",
              "advanced.request-tracker.logs.show-stack-traces = true",
              "profiles.low-threshold.advanced.request-tracker.logs.slow.threshold = 1 nanosecond",
              "profiles.no-logs.advanced.request-tracker.logs.success.enabled = false",
              "profiles.no-logs.advanced.request-tracker.logs.slow.enabled = false",
              "profiles.no-logs.advanced.request-tracker.logs.error.enabled = false",
              "profiles.no-traces.advanced.request-tracker.logs.show-stack-traces = false")
          .build();

  @Rule
  public SessionRule<CqlSession> sessionRuleDefaults =
      SessionRule.builder(simulacronRule)
          .withOptions(
              "advanced.request-tracker.class = com.datastax.oss.driver.internal.core.tracker.RequestLogger",
              "advanced.request-tracker.logs.success.enabled = true",
              "advanced.request-tracker.logs.error.enabled = true",
              "profiles.low-threshold.advanced.request-tracker.logs.slow.threshold = 1 nanosecond",
              "profiles.no-logs.advanced.request-tracker.logs.success.enabled = false",
              "profiles.no-logs.advanced.request-tracker.logs.slow.enabled = false",
              "profiles.no-logs.advanced.request-tracker.logs.error.enabled = false",
              "profiles.no-traces.advanced.request-tracker.logs.show-stack-traces = false")
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
    sessionRuleRequest.session().execute(QUERY);

    // Then
    Mockito.verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .contains("Success", "[0 values]", QUERY);
  }

  @Test
  public void should_log_successful_request_with_defaults() {
    // Given
    simulacronRule.cluster().prime(when(QUERY).then(rows().row("release_version", "3.0.0")));

    // When
    sessionRuleDefaults.session().execute(QUERY);

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
      sessionRuleRequest.session().execute(QUERY);
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
  public void should_log_failed_request_with_stack_trace_with_defaults() {
    // Given
    simulacronRule.cluster().prime(when(QUERY).then(serverError("test")));

    // When
    try {
      sessionRuleDefaults.session().execute(QUERY);
      fail("Expected a ServerError");
    } catch (ServerError error) {
      // expected
    }

    // Then
    Mockito.verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    ILoggingEvent log = loggingEventCaptor.getValue();
    assertThat(log.getFormattedMessage())
        .contains("Error", "[0 values]", QUERY, ServerError.class.getName());
  }

  @Test
  public void should_log_failed_request_without_stack_trace() {
    // Given
    simulacronRule.cluster().prime(when(QUERY).then(serverError("test")));

    // When
    try {
      sessionRuleRequest
          .session()
          .execute(SimpleStatement.builder(QUERY).withExecutionProfileName("no-traces").build());
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
    sessionRuleRequest
        .session()
        .execute(SimpleStatement.builder(QUERY).withExecutionProfileName("low-threshold").build());

    // Then
    Mockito.verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .contains("Slow", "[0 values]", QUERY);
  }

  @Test
  public void should_not_log_when_disabled() throws InterruptedException {
    // Given
    simulacronRule.cluster().prime(when(QUERY).then(rows().row("release_version", "3.0.0")));

    // When
    sessionRuleRequest
        .session()
        .execute(SimpleStatement.builder(QUERY).withExecutionProfileName("no-logs").build());

    // Then
    // We expect no messages. The request logger is invoked asynchronously, so simply wait a bit
    TimeUnit.MILLISECONDS.sleep(500);
    Mockito.verify(appender, never()).doAppend(any(LoggingEvent.class));
  }

  @Test
  public void should_log_failed_nodes_on_successful_request() {
    // Given
    simulacronRule
        .cluster()
        .node(0)
        .prime(when(QUERY).then(unavailable(ConsistencyLevel.ONE, 1, 3)));
    simulacronRule
        .cluster()
        .node(1)
        .prime(when(QUERY).then(rows().row("release_version", "3.0.0")));
    simulacronRule
        .cluster()
        .node(2)
        .prime(when(QUERY).then(rows().row("release_version", "3.0.0")));

    // When
    ResultSet set = sessionRuleNode.session().execute(QUERY);

    // Then
    Mockito.verify(appender, new Timeout(500, VerificationModeFactory.times(3)))
        .doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> events = loggingEventCaptor.getAllValues();
    assertThat(events.get(0).getFormattedMessage()).contains("Error", "[0 values]", QUERY);
    assertThat(events.get(1).getFormattedMessage()).contains("Success", "[0 values]", QUERY);
    assertThat(events.get(2).getFormattedMessage()).contains("Success", "[0 values]", QUERY);
  }

  @Test
  public void should_log_successful_nodes_on_successful_request() {
    simulacronRule
        .cluster()
        .node(0)
        .prime(when(QUERY).then(rows().row("release_version", "3.0.0")));
    simulacronRule
        .cluster()
        .node(1)
        .prime(when(QUERY).then(rows().row("release_version", "3.0.0")));
    simulacronRule
        .cluster()
        .node(2)
        .prime(when(QUERY).then(rows().row("release_version", "3.0.0")));

    // When
    ResultSet set = sessionRuleNode.session().execute(QUERY);

    // Then
    Mockito.verify(appender, new Timeout(500, VerificationModeFactory.times(2)))
        .doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> events = loggingEventCaptor.getAllValues();
    assertThat(events.get(0).getFormattedMessage()).contains("Success", "[0 values]", QUERY);
    assertThat(events.get(1).getFormattedMessage()).contains("Success", "[0 values]", QUERY);
  }
}
