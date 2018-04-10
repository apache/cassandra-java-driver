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
package com.datastax.oss.driver.api.core.session;

import static com.datastax.oss.driver.Assertions.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;

public class LoggingSessionLifecycleListenerTest {

  private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");

  @Mock private DefaultSession session;
  @Mock private DriverContext context;
  @Mock private Appender<ILoggingEvent> appender;
  @Mock private Node node1;
  @Mock private ChannelPool channel;

  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  private Logger logger;
  private Level oldLevel;
  private ConcurrentMap<String, Object> attributes;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    attributes = new ConcurrentHashMap<>();
    when(session.getName()).thenReturn("test");
    when(session.getContext()).thenReturn(context);
    when(session.getKeyspace()).thenReturn(KEYSPACE);
    when(session.getPools()).thenReturn(ImmutableMap.of(node1, channel));
    when(context.getAttributes()).thenReturn(attributes);
    logger = (Logger) LoggerFactory.getLogger(LoggingSessionLifecycleListener.class);
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
  public void should_print_basic_driver_info_before_init() {
    // given
    LoggingSessionLifecycleListener listener = new LoggingSessionLifecycleListener();
    LoggingSessionLifecycleListener.ONLY_ONCE.set(0);

    // when
    listener.beforeInit(session);

    // then
    verify(appender, times(2)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> logs = loggingEventCaptor.getAllValues();
    ILoggingEvent driverInfo = logs.get(0);
    assertThat(driverInfo.getLevel()).isEqualTo(Level.INFO);
    assertThat(driverInfo.getMessage()).isEqualTo("{} loaded (PID: {})");
    assertThat(driverInfo.getFormattedMessage()).contains(Session.getOssDriverInfo().toString());
    ILoggingEvent initializing = logs.get(1);
    assertThat(initializing.getLevel()).isEqualTo(Level.INFO);
    assertThat(initializing.getMessage()).isEqualTo("[{}] Session initializing");
    assertThat(initializing.getFormattedMessage()).contains("[test] Session initializing");
  }

  @Test
  public void should_print_elapsed_time_after_init() {
    // given
    LoggingSessionLifecycleListener listener = new LoggingSessionLifecycleListener();
    attributes.put(
        LoggingSessionLifecycleListener.INIT_START_TIME_KEY,
        System.nanoTime() - SECONDS.toNanos(1));

    // when
    listener.afterInit(session);

    // then
    verify(appender).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> logs = loggingEventCaptor.getAllValues();
    ILoggingEvent initialized = logs.get(0);
    assertThat(initialized.getLevel()).isEqualTo(Level.INFO);
    assertThat(initialized.getFormattedMessage())
        .contains("[test] Session successfully initialized")
        .contains("in 1 s")
        .contains("1 node");
  }

  @Test
  public void should_print_error_when_init_fails() {
    // given
    LoggingSessionLifecycleListener listener = new LoggingSessionLifecycleListener();

    // when
    listener.onInitFailed(session, new NullPointerException("boo"));

    // then
    verify(appender).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> logs = loggingEventCaptor.getAllValues();
    ILoggingEvent closing = logs.get(0);
    assertThat(closing.getLevel()).isEqualTo(Level.ERROR);
    assertThat(closing.getMessage()).isEqualTo("[test] Session failed to initialize");
    assertThat(closing.getThrowableProxy().getMessage()).isEqualTo("boo");
  }

  @Test
  public void should_print_message_before_close() {
    // given
    LoggingSessionLifecycleListener listener = new LoggingSessionLifecycleListener();

    // when
    listener.beforeClose(session);

    // then
    verify(appender).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> logs = loggingEventCaptor.getAllValues();
    ILoggingEvent closing = logs.get(0);
    assertThat(closing.getLevel()).isEqualTo(Level.INFO);
    assertThat(closing.getMessage()).isEqualTo("[{}] Session closing");
  }

  @Test
  public void should_print_elapsed_time_after_close() {
    // given
    LoggingSessionLifecycleListener listener = new LoggingSessionLifecycleListener();
    attributes.put(
        LoggingSessionLifecycleListener.CLOSE_START_TIME_KEY,
        System.nanoTime() - SECONDS.toNanos(1));

    // when
    listener.afterClose(session);

    // then
    verify(appender).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> logs = loggingEventCaptor.getAllValues();
    ILoggingEvent initialized = logs.get(0);
    assertThat(initialized.getLevel()).isEqualTo(Level.INFO);
    assertThat(initialized.getFormattedMessage())
        .contains("[test] Session successfully closed")
        .contains("in 1 s");
  }

  @Test
  public void should_print_error_when_close_fails() {
    // given
    LoggingSessionLifecycleListener listener = new LoggingSessionLifecycleListener();

    // when
    listener.onCloseFailed(session, new NullPointerException("boo"));

    // then
    verify(appender).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> logs = loggingEventCaptor.getAllValues();
    ILoggingEvent closing = logs.get(0);
    assertThat(closing.getLevel()).isEqualTo(Level.ERROR);
    assertThat(closing.getMessage()).isEqualTo("[test] Session failed to close");
    assertThat(closing.getThrowableProxy().getMessage()).isEqualTo("boo");
  }
}
