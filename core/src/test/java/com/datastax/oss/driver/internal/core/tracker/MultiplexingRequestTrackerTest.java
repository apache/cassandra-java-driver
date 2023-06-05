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
package com.datastax.oss.driver.internal.core.tracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.DriverExecutionException;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.Strict.class)
public class MultiplexingRequestTrackerTest {

  @Mock private RequestTracker child1;
  @Mock private RequestTracker child2;
  @Mock private Request request;
  @Mock private DriverExecutionProfile profile;
  @Mock private Node node;
  @Mock private Session session;
  @Mock private ExecutionInfo executionInfo;

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  private Logger logger;
  private Level initialLogLevel;

  private final Exception error = new DriverExecutionException(new NullPointerException());

  @Before
  public void addAppenders() {
    logger = (Logger) LoggerFactory.getLogger(MultiplexingRequestTracker.class);
    initialLogLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    logger.addAppender(appender);
  }

  @After
  public void removeAppenders() {
    logger.detachAppender(appender);
    logger.setLevel(initialLogLevel);
  }

  @Test
  public void should_register() {
    // given
    MultiplexingRequestTracker tracker = new MultiplexingRequestTracker();
    // when
    tracker.register(child1);
    tracker.register(child2);
    // then
    assertThat(tracker).extracting("trackers").asList().hasSize(2).contains(child1, child2);
  }

  @Test
  public void should_flatten_child_multiplexing_tracker_via_constructor() {
    // given
    MultiplexingRequestTracker tracker =
        new MultiplexingRequestTracker(new MultiplexingRequestTracker(child1, child2));
    // when
    // then
    assertThat(tracker).extracting("trackers").asList().hasSize(2).contains(child1, child2);
  }

  @Test
  public void should_flatten_child_multiplexing_tracker_via_register() {
    // given
    MultiplexingRequestTracker tracker = new MultiplexingRequestTracker();
    // when
    tracker.register(new MultiplexingRequestTracker(child1, child2));
    // then
    assertThat(tracker).extracting("trackers").asList().hasSize(2).contains(child1, child2);
  }

  @Test
  public void should_notify_onSuccess() {
    // given
    MultiplexingRequestTracker tracker = new MultiplexingRequestTracker(child1, child2);
    willThrow(new NullPointerException())
        .given(child1)
        .onSuccess(request, 123456L, profile, node, executionInfo, "test");
    // when
    tracker.onSuccess(request, 123456L, profile, node, executionInfo, "test");
    // then
    verify(child1).onSuccess(request, 123456L, profile, node, executionInfo, "test");
    verify(child2).onSuccess(request, 123456L, profile, node, executionInfo, "test");
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "[test] Unexpected error while notifying request tracker child1 of an onSuccess event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onError() {
    // given
    MultiplexingRequestTracker tracker = new MultiplexingRequestTracker(child1, child2);
    willThrow(new NullPointerException())
        .given(child1)
        .onError(request, error, 123456L, profile, node, executionInfo, "test");
    // when
    tracker.onError(request, error, 123456L, profile, node, executionInfo, "test");
    // then
    verify(child1).onError(request, error, 123456L, profile, node, executionInfo, "test");
    verify(child2).onError(request, error, 123456L, profile, node, executionInfo, "test");
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "[test] Unexpected error while notifying request tracker child1 of an onError event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onNodeSuccess() {
    // given
    MultiplexingRequestTracker tracker = new MultiplexingRequestTracker(child1, child2);
    willThrow(new NullPointerException())
        .given(child1)
        .onNodeSuccess(request, 123456L, profile, node, executionInfo, "test");
    // when
    tracker.onNodeSuccess(request, 123456L, profile, node, executionInfo, "test");
    // then
    verify(child1).onNodeSuccess(request, 123456L, profile, node, executionInfo, "test");
    verify(child2).onNodeSuccess(request, 123456L, profile, node, executionInfo, "test");
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "[test] Unexpected error while notifying request tracker child1 of an onNodeSuccess event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onNodeError() {
    // given
    MultiplexingRequestTracker tracker = new MultiplexingRequestTracker(child1, child2);
    willThrow(new NullPointerException())
        .given(child1)
        .onNodeError(request, error, 123456L, profile, node, executionInfo, "test");
    // when
    tracker.onNodeError(request, error, 123456L, profile, node, executionInfo, "test");
    // then
    verify(child1).onNodeError(request, error, 123456L, profile, node, executionInfo, "test");
    verify(child2).onNodeError(request, error, 123456L, profile, node, executionInfo, "test");
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "[test] Unexpected error while notifying request tracker child1 of an onNodeError event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onSessionReady() {
    // given
    MultiplexingRequestTracker tracker = new MultiplexingRequestTracker(child1, child2);
    willThrow(new NullPointerException()).given(child1).onSessionReady(session);
    given(session.getName()).willReturn("test");
    // when
    tracker.onSessionReady(session);
    // then
    verify(child1).onSessionReady(session);
    verify(child2).onSessionReady(session);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "[test] Unexpected error while notifying request tracker child1 of an onSessionReady event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_close() throws Exception {
    // given
    MultiplexingRequestTracker tracker = new MultiplexingRequestTracker(child1, child2);
    Exception child1Error = new NullPointerException();
    willThrow(child1Error).given(child1).close();
    // when
    tracker.close();
    // then
    verify(child1).close();
    verify(child2).close();
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while closing request tracker child1. (NullPointerException: null)");
  }
}
