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

import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METADATA_TOPOLOGY_MAX_EVENTS;
import static com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures.failedFuture;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.datastax.oss.driver.internal.core.metrics.MetricUpdaterFactory;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.PoolManager;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.FailedFuture;
import io.netty.util.concurrent.SucceededFuture;
import java.net.InetSocketAddress;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SessionLifecycleListenerTest {

  private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");

  @Mock private InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile profile;
  @Mock private MetadataManager metadataManager;
  @Mock private TopologyMonitor topologyMonitor;
  @Mock private NettyOptions nettyOptions;
  @Mock private ControlConnection controlConnection;
  @Mock private MetricUpdaterFactory metricUpdaterFactory;
  @Mock private EventBus eventBus;
  @Mock private PoolManager poolManager;
  @Mock private Node node1;
  @Mock private ChannelPool channel;
  @Mock private ReconnectionPolicy reconnectionPolicy;
  @Mock private RetryPolicy retryPolicy;
  @Mock private LoadBalancingPolicyWrapper loadBalancingPolicyWrapper;
  @Mock private SpeculativeExecutionPolicy speculativeExecutionPolicy;
  @Mock private AddressTranslator addressTranslator;
  @Mock private DriverConfigLoader configLoader;
  @Mock private Metadata metadata;
  @Mock private ProtocolVersionRegistry protocolVersionRegistry;
  @Mock private SessionLifecycleListener sessionLifecycleListener;

  @Captor private ArgumentCaptor<Throwable> captor;

  private Set<SessionLifecycleListener> sessionLifecycleListeners;
  private InetSocketAddress address1 = InetSocketAddress.createUnresolved("127.0.0.1", 9042);
  private EventLoop executors;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    executors = new DefaultEventLoop();
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(executors);
    when(poolManager.getPools()).thenReturn(ImmutableMap.of(node1, channel));
    when(context.topologyMonitor()).thenReturn(topologyMonitor);
    when(context.nettyOptions()).thenReturn(nettyOptions);
    when(context.poolManager()).thenReturn(poolManager);
    when(context.config()).thenReturn(config);
    when(context.eventBus()).thenReturn(eventBus);
    when(context.metricUpdaterFactory()).thenReturn(metricUpdaterFactory);
    when(context.reconnectionPolicy()).thenReturn(reconnectionPolicy);
    when(context.retryPolicy()).thenReturn(retryPolicy);
    when(context.loadBalancingPolicyWrapper()).thenReturn(loadBalancingPolicyWrapper);
    when(context.speculativeExecutionPolicy()).thenReturn(speculativeExecutionPolicy);
    when(context.addressTranslator()).thenReturn(addressTranslator);
    when(context.configLoader()).thenReturn(configLoader);
    when(context.protocolVersionRegistry()).thenReturn(protocolVersionRegistry);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getInt(METADATA_TOPOLOGY_MAX_EVENTS)).thenReturn(1);
    when(topologyMonitor.init()).thenReturn(completedFuture(null));
    when(metadataManager.addContactPoints(anySet())).thenReturn(completedFuture(null));
    when(metadataManager.getMetadata()).thenReturn(metadata);
    when(metadata.getNodes()).thenReturn(ImmutableMap.of(address1, node1));
    when(metadataManager.refreshNodes()).thenReturn(completedFuture(null));
    when(metadataManager.firstSchemaRefreshFuture()).thenReturn(completedFuture(null));
    when(protocolVersionRegistry.highestCommon(anyCollection())).thenReturn(V4);
    when(context.metadataManager()).thenReturn(metadataManager);
    when(context.controlConnection()).thenReturn(controlConnection);
    when(context.sessionName()).thenReturn("test");
    when(context.protocolVersion()).thenReturn(V4);
    when(poolManager.closeAsync()).thenReturn(completedFuture(null));
    when(metadataManager.closeAsync()).thenReturn(completedFuture(null));
    when(topologyMonitor.closeAsync()).thenReturn(completedFuture(null));
    when(controlConnection.closeAsync()).thenReturn(completedFuture(null));
    when(poolManager.forceCloseAsync()).thenReturn(completedFuture(null));
    when(metadataManager.forceCloseAsync()).thenReturn(completedFuture(null));
    when(topologyMonitor.forceCloseAsync()).thenReturn(completedFuture(null));
    when(controlConnection.forceCloseAsync()).thenReturn(completedFuture(null));
    when(nettyOptions.onClose()).thenReturn(new SucceededFuture<>(executors.next(), null));
    when(poolManager.init(KEYSPACE)).thenReturn(completedFuture(null));
    sessionLifecycleListeners = new LinkedHashSet<>();
    sessionLifecycleListeners.add(sessionLifecycleListener);
  }

  @Test
  public void should_invoke_listener_when_initializing()
      throws InterruptedException, ExecutionException, TimeoutException {
    // given
    DefaultSession session =
        new DefaultSession(context, emptySet(), emptySet(), sessionLifecycleListeners);

    // when
    CompletionStage<CqlSession> init = session.init(KEYSPACE);
    init.toCompletableFuture().get(10, SECONDS);

    // then
    verify(sessionLifecycleListener).beforeInit(session);
    verify(sessionLifecycleListener, timeout(1000)).afterInit(session);

    verify(sessionLifecycleListener, never()).beforeClose(any(Session.class));
    verify(sessionLifecycleListener, never()).afterClose(any(Session.class));
    verify(sessionLifecycleListener, never())
        .onInitFailed(any(Session.class), any(Throwable.class));
    verify(sessionLifecycleListener, never())
        .onCloseFailed(any(Session.class), any(Throwable.class));
  }

  @Test
  public void should_invoke_listener_when_initializing_fails() {
    // given
    DefaultSession session =
        new DefaultSession(context, emptySet(), emptySet(), sessionLifecycleListeners);
    given(metadataManager.refreshNodes()).willReturn(failedFuture(new NullPointerException()));

    // when
    CompletionStage<CqlSession> init = session.init(KEYSPACE);
    try {
      init.toCompletableFuture().get(10, SECONDS);
      fail("Expecting future to fail");
    } catch (Exception e) {

      // then
      verify(sessionLifecycleListener).beforeInit(session);
      verify(sessionLifecycleListener, timeout(1000)).onInitFailed(eq(session), captor.capture());
      assertThat(captor.getValue()).hasRootCauseExactlyInstanceOf(NullPointerException.class);

      // close sequence is triggered by failed init
      verify(sessionLifecycleListener, timeout(1000)).beforeClose(any(Session.class));
      verify(sessionLifecycleListener, timeout(1000)).afterClose(any(Session.class));

      verify(sessionLifecycleListener, never()).afterInit(any(Session.class));
      verify(sessionLifecycleListener, never())
          .onCloseFailed(any(Session.class), any(Throwable.class));
    }
  }

  @Test
  public void should_invoke_listener_when_closing()
      throws InterruptedException, ExecutionException, TimeoutException {
    // given
    DefaultSession session =
        new DefaultSession(context, emptySet(), emptySet(), sessionLifecycleListeners);

    // when
    CompletionStage<Void> close = session.closeAsync();
    close.toCompletableFuture().get(10, SECONDS);

    // then
    verify(sessionLifecycleListener).beforeClose(session);
    verify(sessionLifecycleListener, timeout(1000)).afterClose(any(Session.class));

    verify(sessionLifecycleListener, never()).beforeInit(any(Session.class));
    verify(sessionLifecycleListener, never()).afterInit(any(Session.class));
    verify(sessionLifecycleListener, never())
        .onInitFailed(any(Session.class), any(Throwable.class));
    verify(sessionLifecycleListener, never())
        .onCloseFailed(any(Session.class), any(Throwable.class));
  }

  @Test
  public void should_invoke_listener_when_closing_fails() {
    // given
    DefaultSession session =
        new DefaultSession(context, emptySet(), emptySet(), sessionLifecycleListeners);
    given(nettyOptions.onClose())
        .willReturn(new FailedFuture<>(executors.next(), new NullPointerException()));

    // when
    CompletionStage<Void> close = session.closeAsync();
    try {
      close.toCompletableFuture().get(10, SECONDS);
      fail("Expecting future to fail");
    } catch (Exception e) {

      // then
      verify(sessionLifecycleListener).beforeClose(session);
      verify(sessionLifecycleListener, timeout(1000)).onCloseFailed(eq(session), captor.capture());
      assertThat(captor.getValue()).isInstanceOf(NullPointerException.class);

      verify(sessionLifecycleListener, never()).beforeInit(any(Session.class));
      verify(sessionLifecycleListener, never()).afterInit(any(Session.class));
      verify(sessionLifecycleListener, never())
          .onInitFailed(any(Session.class), any(Throwable.class));
      verify(sessionLifecycleListener, never()).afterClose(any(Session.class));
    }
  }

  @Test
  public void should_invoke_listener_when_force_closing()
      throws InterruptedException, ExecutionException, TimeoutException {
    // given
    DefaultSession session =
        new DefaultSession(context, emptySet(), emptySet(), sessionLifecycleListeners);

    // when
    CompletionStage<Void> close = session.forceCloseAsync();
    close.toCompletableFuture().get(10, SECONDS);

    // then
    verify(sessionLifecycleListener).beforeClose(session);
    verify(sessionLifecycleListener, timeout(1000)).afterClose(any(Session.class));

    verify(sessionLifecycleListener, never()).beforeInit(any(Session.class));
    verify(sessionLifecycleListener, never()).afterInit(any(Session.class));
    verify(sessionLifecycleListener, never())
        .onInitFailed(any(Session.class), any(Throwable.class));
    verify(sessionLifecycleListener, never())
        .onCloseFailed(any(Session.class), any(Throwable.class));
  }

  @Test
  public void should_invoke_listener_when_force_closing_fails() {
    // given
    DefaultSession session =
        new DefaultSession(context, emptySet(), emptySet(), sessionLifecycleListeners);
    given(nettyOptions.onClose())
        .willReturn(new FailedFuture<>(executors.next(), new NullPointerException()));

    // when
    CompletionStage<Void> close = session.forceCloseAsync();
    try {
      close.toCompletableFuture().get(10, SECONDS);
      fail("Expecting future to fail");
    } catch (Exception e) {

      // then
      verify(sessionLifecycleListener).beforeClose(session);
      verify(sessionLifecycleListener, timeout(1000)).onCloseFailed(eq(session), captor.capture());
      assertThat(captor.getValue()).isInstanceOf(NullPointerException.class);

      verify(sessionLifecycleListener, never()).beforeInit(any(Session.class));
      verify(sessionLifecycleListener, never()).afterInit(any(Session.class));
      verify(sessionLifecycleListener, never())
          .onInitFailed(any(Session.class), any(Throwable.class));
      verify(sessionLifecycleListener, never()).afterClose(any(Session.class));
    }
  }
}
