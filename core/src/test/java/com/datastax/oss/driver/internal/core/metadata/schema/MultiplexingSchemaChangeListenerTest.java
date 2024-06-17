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
package com.datastax.oss.driver.internal.core.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
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
public class MultiplexingSchemaChangeListenerTest {

  @Mock private SchemaChangeListener child1;
  @Mock private SchemaChangeListener child2;
  @Mock private Session session;
  @Mock private KeyspaceMetadata keyspace1, keyspace2;
  @Mock private TableMetadata table1, table2;
  @Mock private UserDefinedType userDefinedType1, userDefinedType2;
  @Mock private FunctionMetadata function1, function2;
  @Mock private AggregateMetadata aggregate1, aggregate2;
  @Mock private ViewMetadata view1, view2;

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  private Logger logger;
  private Level initialLogLevel;

  @Before
  public void addAppenders() {
    logger = (Logger) LoggerFactory.getLogger(MultiplexingSchemaChangeListener.class);
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
    MultiplexingSchemaChangeListener listener = new MultiplexingSchemaChangeListener();
    // when
    listener.register(child1);
    listener.register(child2);
    // then
    assertThat(listener).extracting("listeners").asList().hasSize(2).contains(child1, child2);
  }

  @Test
  public void should_flatten_child_multiplexing_listener_via_constructor() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(new MultiplexingSchemaChangeListener(child1, child2));
    // when
    // then
    assertThat(listener).extracting("listeners").asList().hasSize(2).contains(child1, child2);
  }

  @Test
  public void should_flatten_child_multiplexing_listener_via_register() {
    // given
    MultiplexingSchemaChangeListener listener = new MultiplexingSchemaChangeListener();
    // when
    listener.register(new MultiplexingSchemaChangeListener(child1, child2));
    // then
    assertThat(listener).extracting("listeners").asList().hasSize(2).contains(child1, child2);
  }

  @Test
  public void should_notify_onKeyspaceCreated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onKeyspaceCreated(keyspace1);
    // when
    listener.onKeyspaceCreated(keyspace1);
    // then
    verify(child1).onKeyspaceCreated(keyspace1);
    verify(child2).onKeyspaceCreated(keyspace1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onKeyspaceCreated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onKeyspaceDropped() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onKeyspaceDropped(keyspace1);
    // when
    listener.onKeyspaceDropped(keyspace1);
    // then
    verify(child1).onKeyspaceDropped(keyspace1);
    verify(child2).onKeyspaceDropped(keyspace1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onKeyspaceDropped event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onKeyspaceUpdated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onKeyspaceUpdated(keyspace1, keyspace2);
    // when
    listener.onKeyspaceUpdated(keyspace1, keyspace2);
    // then
    verify(child1).onKeyspaceUpdated(keyspace1, keyspace2);
    verify(child2).onKeyspaceUpdated(keyspace1, keyspace2);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onKeyspaceUpdated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onTableCreated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onTableCreated(table1);
    // when
    listener.onTableCreated(table1);
    // then
    verify(child1).onTableCreated(table1);
    verify(child2).onTableCreated(table1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onTableCreated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onTableDropped() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onTableDropped(table1);
    // when
    listener.onTableDropped(table1);
    // then
    verify(child1).onTableDropped(table1);
    verify(child2).onTableDropped(table1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onTableDropped event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onTableUpdated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onTableUpdated(table1, table2);
    // when
    listener.onTableUpdated(table1, table2);
    // then
    verify(child1).onTableUpdated(table1, table2);
    verify(child2).onTableUpdated(table1, table2);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onTableUpdated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onUserDefinedTypeCreated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onUserDefinedTypeCreated(userDefinedType1);
    // when
    listener.onUserDefinedTypeCreated(userDefinedType1);
    // then
    verify(child1).onUserDefinedTypeCreated(userDefinedType1);
    verify(child2).onUserDefinedTypeCreated(userDefinedType1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onUserDefinedTypeCreated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onUserDefinedTypeDropped() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onUserDefinedTypeDropped(userDefinedType1);
    // when
    listener.onUserDefinedTypeDropped(userDefinedType1);
    // then
    verify(child1).onUserDefinedTypeDropped(userDefinedType1);
    verify(child2).onUserDefinedTypeDropped(userDefinedType1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onUserDefinedTypeDropped event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onUserDefinedTypeUpdated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException())
        .given(child1)
        .onUserDefinedTypeUpdated(userDefinedType1, userDefinedType2);
    // when
    listener.onUserDefinedTypeUpdated(userDefinedType1, userDefinedType2);
    // then
    verify(child1).onUserDefinedTypeUpdated(userDefinedType1, userDefinedType2);
    verify(child2).onUserDefinedTypeUpdated(userDefinedType1, userDefinedType2);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onUserDefinedTypeUpdated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onFunctionCreated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onFunctionCreated(function1);
    // when
    listener.onFunctionCreated(function1);
    // then
    verify(child1).onFunctionCreated(function1);
    verify(child2).onFunctionCreated(function1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onFunctionCreated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onFunctionDropped() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onFunctionDropped(function1);
    // when
    listener.onFunctionDropped(function1);
    // then
    verify(child1).onFunctionDropped(function1);
    verify(child2).onFunctionDropped(function1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onFunctionDropped event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onFunctionUpdated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onFunctionUpdated(function1, function2);
    // when
    listener.onFunctionUpdated(function1, function2);
    // then
    verify(child1).onFunctionUpdated(function1, function2);
    verify(child2).onFunctionUpdated(function1, function2);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onFunctionUpdated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onAggregateCreated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onAggregateCreated(aggregate1);
    // when
    listener.onAggregateCreated(aggregate1);
    // then
    verify(child1).onAggregateCreated(aggregate1);
    verify(child2).onAggregateCreated(aggregate1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onAggregateCreated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onAggregateDropped() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onAggregateDropped(aggregate1);
    // when
    listener.onAggregateDropped(aggregate1);
    // then
    verify(child1).onAggregateDropped(aggregate1);
    verify(child2).onAggregateDropped(aggregate1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onAggregateDropped event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onAggregateUpdated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onAggregateUpdated(aggregate1, aggregate2);
    // when
    listener.onAggregateUpdated(aggregate1, aggregate2);
    // then
    verify(child1).onAggregateUpdated(aggregate1, aggregate2);
    verify(child2).onAggregateUpdated(aggregate1, aggregate2);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onAggregateUpdated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onViewCreated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onViewCreated(view1);
    // when
    listener.onViewCreated(view1);
    // then
    verify(child1).onViewCreated(view1);
    verify(child2).onViewCreated(view1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onViewCreated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onViewDropped() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onViewDropped(view1);
    // when
    listener.onViewDropped(view1);
    // then
    verify(child1).onViewDropped(view1);
    verify(child2).onViewDropped(view1);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onViewDropped event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onViewUpdated() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onViewUpdated(view1, view2);
    // when
    listener.onViewUpdated(view1, view2);
    // then
    verify(child1).onViewUpdated(view1, view2);
    verify(child2).onViewUpdated(view1, view2);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onViewUpdated event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_onSessionReady() {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
    willThrow(new NullPointerException()).given(child1).onSessionReady(session);
    // when
    listener.onSessionReady(session);
    // then
    verify(child1).onSessionReady(session);
    verify(child2).onSessionReady(session);
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues().stream().map(ILoggingEvent::getFormattedMessage))
        .contains(
            "Unexpected error while notifying schema change listener child1 of an onSessionReady event. (NullPointerException: null)");
  }

  @Test
  public void should_notify_close() throws Exception {
    // given
    MultiplexingSchemaChangeListener listener =
        new MultiplexingSchemaChangeListener(child1, child2);
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
            "Unexpected error while closing schema change listener child1. (NullPointerException: null)");
  }
}
