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
package com.datastax.oss.driver.internal.core.session;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METADATA_TOPOLOGY_MAX_EVENTS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.STARTUP_CUSTOM_BANNER;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.STARTUP_PRINT_BASIC_INFO;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverInfo;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.DefaultDriverInfo;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metrics.MetricUpdaterFactory;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.DefaultEventLoopGroup;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;

public class SessionStartupTest {

  private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");

  private static final String BANNER = " --- My custom banner --- ";

  private static final DriverInfo DRIVER_INFO =
      DefaultDriverInfo.buildFromResource(
          SessionStartupTest.class.getResource("/com/datastax/oss/driver/Driver.properties"));

  @Mock private InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile profile;
  @Mock private MetadataManager metadataManager;
  @Mock private NettyOptions nettyOptions;
  @Mock private MetricUpdaterFactory metricUpdaterFactory;
  @Mock private EventBus eventBus;
  @Mock private PoolManager poolManager;
  @Mock private Node node1;
  @Mock private ChannelPool channel;
  @Mock private Appender<ILoggingEvent> appender;

  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  private Logger logger;
  private Level oldLevel;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(new DefaultEventLoopGroup(1));
    when(poolManager.getPools()).thenReturn(ImmutableMap.of(node1, channel));
    when(context.nettyOptions()).thenReturn(nettyOptions);
    when(context.poolManager()).thenReturn(poolManager);
    when(context.config()).thenReturn(config);
    when(context.eventBus()).thenReturn(eventBus);
    when(context.metricUpdaterFactory()).thenReturn(metricUpdaterFactory);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getInt(METADATA_TOPOLOGY_MAX_EVENTS)).thenReturn(1);
    when(metadataManager.addContactPoints(anySet())).thenReturn(completedFuture(null));
    when(metadataManager.refreshNodes()).thenReturn(completedFuture(null));
    when(metadataManager.firstSchemaRefreshFuture()).thenReturn(completedFuture(null));
    when(context.metadataManager()).thenReturn(metadataManager);
    when(context.sessionName()).thenReturn("test");
    logger = (Logger) LoggerFactory.getLogger("com.datastax.oss.driver.api.core.Startup");
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
  public void should_print_basic_info_if_enabled() {
    // given
    given(profile.getBoolean(STARTUP_PRINT_BASIC_INFO)).willReturn(true);
    DefaultSession session = new DefaultSession(context, emptySet(), emptySet());

    // when
    CompletionStage<CqlSession> init = session.init(KEYSPACE);
    init.toCompletableFuture().complete(null);

    // then
    verify(appender, times(2)).doAppend(loggingEventCaptor.capture());
    List<ILoggingEvent> logs = loggingEventCaptor.getAllValues();
    ILoggingEvent initializing = logs.get(0);
    assertThat(initializing.getLevel()).isEqualTo(Level.INFO);
    assertThat(initializing.getMessage()).isEqualTo("[{}] {} initializing (PID: {})");
    assertThat(initializing.getFormattedMessage())
        .contains("[test]")
        .contains(DRIVER_INFO.toString());
    ILoggingEvent initialized = logs.get(1);
    assertThat(initialized.getLevel()).isEqualTo(Level.INFO);
    assertThat(initialized.getMessage())
        .isEqualTo("[{}] Session successfully initialized in {}, connected to {} node{}");
    assertThat(initialized.getFormattedMessage()).contains("[test]").contains("1 node");
  }

  @Test
  public void should_not_print_basic_info_if_disabled() {
    // given
    given(profile.getBoolean(STARTUP_PRINT_BASIC_INFO)).willReturn(false);
    DefaultSession session = new DefaultSession(context, emptySet(), emptySet());

    // when
    session.init(KEYSPACE);

    // then
    verify(appender, never()).doAppend(loggingEventCaptor.capture());
  }

  @Test
  public void should_print_custom_banner_if_defined() {
    // given
    given(profile.isDefined(STARTUP_CUSTOM_BANNER)).willReturn(true);
    given(profile.getString(STARTUP_CUSTOM_BANNER)).willReturn(BANNER);
    DefaultSession session = new DefaultSession(context, emptySet(), emptySet());

    // when
    session.init(KEYSPACE);

    // then
    verify(appender).doAppend(loggingEventCaptor.capture());
    ILoggingEvent log = loggingEventCaptor.getValue();
    assertThat(log.getLevel()).isEqualTo(Level.INFO);
    assertThat(log.getMessage()).isEqualTo(BANNER);
  }

  @Test
  public void should_not_print_custom_banner_if_undefined() {
    // given
    given(profile.isDefined(STARTUP_CUSTOM_BANNER)).willReturn(false);
    DefaultSession session = new DefaultSession(context, emptySet(), emptySet());

    // when
    session.init(KEYSPACE);

    // then
    verify(appender, never()).doAppend(loggingEventCaptor.capture());
  }
}
