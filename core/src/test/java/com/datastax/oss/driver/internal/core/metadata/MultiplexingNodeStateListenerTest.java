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
package com.datastax.oss.driver.internal.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.session.Session;
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
public class MultiplexingNodeStateListenerTest {

  @Mock private NodeStateListener child1;
  @Mock private NodeStateListener child2;
  @Mock private Node node;
  @Mock private Session session;

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  private Logger logger;
  private Level initialLogLevel;

  @Before
  public void addAppenders() {
    logger = (Logger) LoggerFactory.getLogger(MultiplexingNodeStateListener.class);
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
    MultiplexingNodeStateListener listener = new MultiplexingNodeStateListener();
    // when
    listener.register(child1);
    listener.register(child2);
    // then
    assertThat(listener).extracting("listeners").asList().hasSize(2).contains(child1, child2);
  }

  @Test
  public void should_flatten_child_multiplexing_listener_via_constructor() {
    // given
    MultiplexingNodeStateListener listener =
        new MultiplexingNodeStateListener(new MultiplexingNodeStateListener(child1, child2));
    // when
    // then
    assertThat(listener).extracting("listeners").asList().hasSize(2).contains(child1, child2);
  }

  @Test
  public void should_flatten_child_multiplexing_listener_via_register() {
    // given
    MultiplexingNodeStateListener listener = new MultiplexingNodeStateListener();
    // when
    listener.register(new MultiplexingNodeStateListener(child1, child2));
    // then
    assertThat(listener).extracting("listeners").asList().hasSize(2).contains(child1, child2);
  }

  @Test
  public void should_notify_onUp() {
    // given
    MultiplexingNodeStateListener listener = new MultiplexingNodeStateListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onUp(node);
    // when
    listener.onUp(node);
    // then
    verify(child1).onUp(node);
    verify(child2).onUp(node);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying node state listener child1 of an onUp event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onDown() {
    // given
    MultiplexingNodeStateListener listener = new MultiplexingNodeStateListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onDown(node);
    // when
    listener.onDown(node);
    // then
    verify(child1).onDown(node);
    verify(child2).onDown(node);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying node state listener child1 of an onDown event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onAdd() {
    // given
    MultiplexingNodeStateListener listener = new MultiplexingNodeStateListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onAdd(node);
    // when
    listener.onAdd(node);
    // then
    verify(child1).onAdd(node);
    verify(child2).onAdd(node);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying node state listener child1 of an onAdd event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onRemove() {
    // given
    MultiplexingNodeStateListener listener = new MultiplexingNodeStateListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onRemove(node);
    // when
    listener.onRemove(node);
    // then
    verify(child1).onRemove(node);
    verify(child2).onRemove(node);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying node state listener child1 of an onRemove event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onSessionReady() {
    // given
    MultiplexingNodeStateListener listener = new MultiplexingNodeStateListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onSessionReady(session);
    // when
    listener.onSessionReady(session);
    // then
    verify(child1).onSessionReady(session);
    verify(child2).onSessionReady(session);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying node state listener child1 of an onSessionReady event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_close() throws Exception {
    // given
    MultiplexingNodeStateListener listener = new MultiplexingNodeStateListener(child1, child2);
    Exception child1Error = new NullPointerException();
    willThrow(child1Error).given(child1).close();
    // when
    listener.close();
    // then
    verify(child1).close();
    verify(child2).close();
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while closing node state listener child1. (NullPointerException: null)");
  }
}
