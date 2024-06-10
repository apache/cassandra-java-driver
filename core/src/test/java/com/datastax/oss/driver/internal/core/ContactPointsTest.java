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
package com.datastax.oss.driver.internal.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.filter;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class ContactPointsTest {

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;
  private Logger logger;
  private Level initialLogLevel;

  @Before
  public void setup() {
    logger = (Logger) LoggerFactory.getLogger(ContactPoints.class);
    initialLogLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    logger.addAppender(appender);
  }

  @After
  public void teardown() {
    logger.detachAppender(appender);
    logger.setLevel(initialLogLevel);
  }

  @Test
  public void should_parse_ipv4_address_and_port_in_configuration() {
    Set<EndPoint> endPoints =
        ContactPoints.merge(Collections.emptySet(), ImmutableList.of("127.0.0.1:9042"), true);

    assertThat(endPoints)
        .containsExactly(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042)));
  }

  @Test
  public void should_parse_ipv6_address_and_port_in_configuration() {
    Set<EndPoint> endPoints =
        ContactPoints.merge(
            Collections.emptySet(), ImmutableList.of("0:0:0:0:0:0:0:1:9042", "::2:9042"), true);

    assertThat(endPoints)
        .containsExactly(
            new DefaultEndPoint(new InetSocketAddress("::1", 9042)),
            new DefaultEndPoint(new InetSocketAddress("::2", 9042)));
  }

  @Test
  public void should_parse_host_and_port_in_configuration_and_create_unresolved() {
    Set<EndPoint> endPoints =
        ContactPoints.merge(Collections.emptySet(), ImmutableList.of("localhost:9042"), false);

    assertThat(endPoints)
        .containsExactly(
            new DefaultEndPoint(InetSocketAddress.createUnresolved("localhost", 9042)));
  }

  @Test
  public void should_parse_host_and_port_and_resolve_all_a_records() throws UnknownHostException {
    int localhostARecordsCount = InetAddress.getAllByName("localhost").length;
    assumeTrue(
        "This test assumes that localhost resolves to multiple A-records",
        localhostARecordsCount >= 2);

    Set<EndPoint> endPoints =
        ContactPoints.merge(Collections.emptySet(), ImmutableList.of("localhost:9042"), true);

    assertThat(endPoints).hasSize(localhostARecordsCount);
    assertLog(
        Level.INFO,
        "Contact point localhost:9042 resolves to multiple addresses, will use them all");
  }

  @Test
  public void should_ignore_malformed_host_and_port_and_warn() {
    Set<EndPoint> endPoints =
        ContactPoints.merge(Collections.emptySet(), ImmutableList.of("foobar"), true);

    assertThat(endPoints).isEmpty();
    assertLog(Level.WARN, "Ignoring invalid contact point foobar (expecting host:port)");
  }

  @Test
  public void should_ignore_malformed_port_and_warn() {
    Set<EndPoint> endPoints =
        ContactPoints.merge(Collections.emptySet(), ImmutableList.of("127.0.0.1:foobar"), true);

    assertThat(endPoints).isEmpty();
    assertLog(
        Level.WARN,
        "Ignoring invalid contact point 127.0.0.1:foobar (expecting a number, got foobar)");
  }

  @Test
  public void should_merge_programmatic_and_configuration() {
    Set<EndPoint> endPoints =
        ContactPoints.merge(
            ImmutableSet.of(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042))),
            ImmutableList.of("127.0.0.2:9042"),
            true);

    assertThat(endPoints)
        .containsOnly(
            new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042)),
            new DefaultEndPoint(new InetSocketAddress("127.0.0.2", 9042)));
  }

  @Test
  public void should_warn_if_duplicate_between_programmatic_and_configuration() {
    Set<EndPoint> endPoints =
        ContactPoints.merge(
            ImmutableSet.of(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042))),
            ImmutableList.of("127.0.0.1:9042"),
            true);

    assertThat(endPoints)
        .containsOnly(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042)));
    assertLog(Level.WARN, "Duplicate contact point /127.0.0.1:9042");
  }

  @Test
  public void should_warn_if_duplicate_in_configuration() {
    Set<EndPoint> endPoints =
        ContactPoints.merge(
            Collections.emptySet(), ImmutableList.of("127.0.0.1:9042", "127.0.0.1:9042"), true);

    assertThat(endPoints)
        .containsOnly(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042)));
    assertLog(Level.WARN, "Duplicate contact point /127.0.0.1:9042");
  }

  private void assertLog(Level level, String message) {
    verify(appender, atLeast(1)).doAppend(loggingEventCaptor.capture());
    Iterable<ILoggingEvent> logs =
        filter(loggingEventCaptor.getAllValues()).with("level", level).get();
    assertThat(logs).hasSize(1);
    assertThat(logs.iterator().next().getFormattedMessage()).contains(message);
  }
}
