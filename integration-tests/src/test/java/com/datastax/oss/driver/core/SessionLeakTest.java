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
package com.datastax.oss.driver.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.HashSet;
import java.util.Set;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@Category(IsolatedTests.class)
@RunWith(MockitoJUnitRunner.class)
public class SessionLeakTest {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  @Before
  public void setupLogger() {
    Logger logger = (Logger) LoggerFactory.getLogger(DefaultSession.class);
    logger.setLevel(Level.WARN);
    logger.addAppender(appender);
    // no need to clean up after since this is an isolated test
  }

  @Test
  public void should_warn_when_session_count_exceeds_threshold() {
    int threshold = 4;
    // Set the config option explicitly, in case it gets overridden in the test application.conf:
    DriverConfigLoader configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withInt(DefaultDriverOption.SESSION_LEAK_THRESHOLD, threshold)
            .build();

    Set<CqlSession> sessions = new HashSet<>();

    // Stay under the threshold, no warnings expected
    for (int i = 0; i < threshold; i++) {
      sessions.add(SessionUtils.newSession(SIMULACRON_RULE, configLoader));
    }
    verify(appender, never()).doAppend(any());

    // Go over the threshold, 1 warning for every new session
    sessions.add(SessionUtils.newSession(SIMULACRON_RULE, configLoader));
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .contains("You have too many session instances: 5 active, expected less than 4");

    reset(appender);
    sessions.add(SessionUtils.newSession(SIMULACRON_RULE, configLoader));
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .contains("You have too many session instances: 6 active, expected less than 4");

    // Go back under the threshold, no warnings expected
    sessions.forEach(Session::close);
    sessions.clear();
    reset(appender);
    CqlSession session = SessionUtils.newSession(SIMULACRON_RULE, configLoader);
    verify(appender, never()).doAppend(any());
    session.close();
  }
}
