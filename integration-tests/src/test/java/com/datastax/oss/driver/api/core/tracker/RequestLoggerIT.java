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
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.tracker.RequestLogger;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
import org.mockito.internal.verification.VerificationModeFactory;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.verification.Timeout;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class RequestLoggerIT {

  private static final String QUERY = "SELECT release_version FROM system.local";

  private SimulacronRule simulacronRule = new SimulacronRule(ClusterSpec.builder().withNodes(3));

  private final DefaultDriverConfigLoaderBuilder.Profile lowThresholdProfile =
      DefaultDriverConfigLoaderBuilder.profileBuilder()
          .withDuration(DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD, Duration.ofNanos(1))
          .build();

  private final DefaultDriverConfigLoaderBuilder.Profile noLogsProfile =
      DefaultDriverConfigLoaderBuilder.profileBuilder()
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED, false)
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED, false)
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED, false)
          .build();

  private final DefaultDriverConfigLoaderBuilder.Profile noTracesProfile =
      DefaultDriverConfigLoaderBuilder.profileBuilder()
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES, false)
          .build();

  private final DriverConfigLoader requestLoader =
      SessionUtils.configLoaderBuilder()
          .withClass(DefaultDriverOption.REQUEST_TRACKER_CLASS, RequestLogger.class)
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED, true)
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED, true)
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED, true)
          .withInt(
              DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
              RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH)
          .withBoolean(
              DefaultDriverOption.REQUEST_LOGGER_VALUES,
              RequestLogger.DEFAULT_REQUEST_LOGGER_SHOW_VALUES)
          .withInt(
              DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH,
              RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUE_LENGTH)
          .withInt(
              DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES,
              RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUES)
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES, true)
          .withProfile("low-threshold", lowThresholdProfile)
          .withProfile("no-logs", noLogsProfile)
          .withProfile("no-traces", noTracesProfile)
          .build();

  private SessionRule<CqlSession> sessionRuleRequest =
      SessionRule.builder(simulacronRule).withConfigLoader(requestLoader).build();

  private final DriverConfigLoader nodeLoader =
      SessionUtils.configLoaderBuilder()
          .withClass(DefaultDriverOption.REQUEST_TRACKER_CLASS, RequestNodeLoggerExample.class)
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED, true)
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED, true)
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED, true)
          .withInt(
              DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
              RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH)
          .withBoolean(
              DefaultDriverOption.REQUEST_LOGGER_VALUES,
              RequestLogger.DEFAULT_REQUEST_LOGGER_SHOW_VALUES)
          .withInt(
              DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH,
              RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUE_LENGTH)
          .withInt(
              DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES,
              RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUES)
          .withBoolean(DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES, true)
          .withProfile("low-threshold", lowThresholdProfile)
          .withProfile("no-logs", noLogsProfile)
          .withProfile("no-traces", noTracesProfile)
          .build();

  private SessionRule<CqlSession> sessionRuleNode =
      SessionRule.builder(simulacronRule).withConfigLoader(nodeLoader).build();

  private SessionRule<CqlSession> sessionRuleDefaults =
      SessionRule.builder(simulacronRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withClass(DefaultDriverOption.REQUEST_TRACKER_CLASS, RequestLogger.class)
                  .withBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED, true)
                  .withBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED, true)
                  .withProfile("low-threshold", lowThresholdProfile)
                  .withProfile("no-logs", noLogsProfile)
                  .withProfile("no-traces", noTracesProfile)
                  .build())
          .build();

  @Rule
  public TestRule chain =
      RuleChain.outerRule(simulacronRule)
          .around(sessionRuleRequest)
          .around(sessionRuleNode)
          .around(sessionRuleDefaults);

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
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
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
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
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
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
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
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
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
          .execute(SimpleStatement.builder(QUERY).setExecutionProfileName("no-traces").build());
      fail("Expected a ServerError");
    } catch (ServerError error) {
      // expected
    }

    // Then
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
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
        .execute(SimpleStatement.builder(QUERY).setExecutionProfileName("low-threshold").build());

    // Then
    verify(appender, timeout(500)).doAppend(loggingEventCaptor.capture());
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
        .execute(SimpleStatement.builder(QUERY).setExecutionProfileName("no-logs").build());

    // Then
    // We expect no messages. The request logger is invoked asynchronously, so simply wait a bit
    TimeUnit.MILLISECONDS.sleep(500);
    verify(appender, never()).doAppend(any(LoggingEvent.class));
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
    verify(appender, new Timeout(500, VerificationModeFactory.times(3)))
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
    verify(appender, new Timeout(500, VerificationModeFactory.times(2)))
        .doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> events = loggingEventCaptor.getAllValues();
    assertThat(events.get(0).getFormattedMessage()).contains("Success", "[0 values]", QUERY);
    assertThat(events.get(1).getFormattedMessage()).contains("Success", "[0 values]", QUERY);
  }
}
