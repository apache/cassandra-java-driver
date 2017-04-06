/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.pool;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy.ReconnectionSchedule;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.ClusterNameMismatchException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

public class ChannelPoolTest {
  private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9042);

  private @Mock InternalDriverContext context;
  private @Mock ReconnectionPolicy reconnectionPolicy;
  private @Mock ReconnectionSchedule reconnectionSchedule;
  private @Mock NettyOptions nettyOptions;
  private @Mock EventBus eventBus;
  private @Mock ChannelFactory channelFactory;
  private DefaultEventLoopGroup adminEventLoopGroup;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);

    adminEventLoopGroup = new DefaultEventLoopGroup(1);

    Mockito.when(context.nettyOptions()).thenReturn(nettyOptions);
    Mockito.when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    Mockito.when(context.eventBus()).thenReturn(eventBus);
    Mockito.when(context.channelFactory()).thenReturn(channelFactory);

    Mockito.when(context.reconnectionPolicy()).thenReturn(reconnectionPolicy);
    Mockito.when(reconnectionPolicy.newSchedule()).thenReturn(reconnectionSchedule);
    // By default, set a large reconnection delay. Tests that care about reconnection will override
    // it.
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));
  }

  @AfterMethod
  public void teardown() {
    adminEventLoopGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
  }

  @Test
  public void should_initialize_when_all_channels_succeed() throws Exception {
    int poolSize = 3;

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .success(ADDRESS, channel3)
            .build();

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();

    assertThat(poolFuture)
        .isSuccess(pool -> assertThat(pool.channels).containsOnly(channel1, channel2, channel3));
    Mockito.verify(eventBus, times(3)).fire(ChannelEvent.channelOpened(ADDRESS));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_initialize_when_all_channels_fail() throws Exception {
    int poolSize = 3;

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(ADDRESS, "mock channel init failure")
            .failure(ADDRESS, "mock channel init failure")
            .failure(ADDRESS, "mock channel init failure")
            .build();

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess(pool -> assertThat(pool.channels).isEmpty());
    Mockito.verify(eventBus, never()).fire(ChannelEvent.channelOpened(ADDRESS));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_fire_force_down_event_when_cluster_name_does_not_match() throws Exception {
    int poolSize = 3;

    ClusterNameMismatchException error =
        new ClusterNameMismatchException(ADDRESS, "actual", "expected");
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(ADDRESS, error)
            .failure(ADDRESS, error)
            .failure(ADDRESS, error)
            .build();

    ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();

    Mockito.verify(eventBus).fire(TopologyEvent.forceDown(ADDRESS));
    Mockito.verify(eventBus, never()).fire(ChannelEvent.channelOpened(ADDRESS));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_when_init_incomplete() throws Exception {
    // Short delay so we don't have to wait in the test
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int poolSize = 2;

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // Init: 1 channel fails, the other succeeds
            .failure(ADDRESS, "mock channel init failure")
            .success(ADDRESS, channel1)
            // 1st reconnection
            .pending(ADDRESS, channel2Future)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1);
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS));

    // A reconnection should have been scheduled
    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(ADDRESS));

    channel2Future.complete(channel2);
    factoryHelper.waitForCalls(ADDRESS, 1);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStopped(ADDRESS));

    assertThat(pool.channels).containsOnly(channel1, channel2);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_when_channel_dies() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int poolSize = 2;

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    CompletableFuture<DriverChannel> channel3Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            // reconnection
            .pending(ADDRESS, channel3Future)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(ADDRESS));

    // Simulate fatal error on channel2
    ((ChannelPromise) channel2.closeFuture())
        .setFailure(new Exception("mock channel init failure"));
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelClosed(ADDRESS));

    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(ADDRESS));
    factoryHelper.waitForCall(ADDRESS);

    channel3Future.complete(channel3);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS));
    Mockito.verify(eventBus).fire(ChannelEvent.reconnectionStopped(ADDRESS));

    assertThat(pool.channels).containsOnly(channel1, channel3);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_shrink_outside_of_reconnection() throws Exception {
    int poolSize = 4;

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .success(ADDRESS, channel3)
            .success(ADDRESS, channel4)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 4);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);
    inOrder.verify(eventBus, times(4)).fire(ChannelEvent.channelOpened(ADDRESS));

    pool.resize(2);

    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelClosed(ADDRESS));

    assertThat(pool.channels).containsOnly(channel3, channel4);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_shrink_during_reconnection() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int poolSize = 4;

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    CompletableFuture<DriverChannel> channel3Future = new CompletableFuture<>();
    DriverChannel channel4 = newMockDriverChannel(4);
    CompletableFuture<DriverChannel> channel4Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .failure(ADDRESS, "mock channel init failure")
            .failure(ADDRESS, "mock channel init failure")
            // reconnection
            .pending(ADDRESS, channel3Future)
            .pending(ADDRESS, channel4Future)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 4);
    waitForPendingAdminTasks();

    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(ADDRESS));
    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    // A reconnection should have been scheduled to add the missing channels, don't complete yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(ADDRESS));

    pool.resize(2);

    waitForPendingAdminTasks();

    // Now allow the reconnected channels to complete initialization
    channel3Future.complete(channel3);
    channel4Future.complete(channel4);

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    // Pool should have shrinked back to 2. We keep the most recent channels so 1 and 2 get closed.
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(ADDRESS));
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelClosed(ADDRESS));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStopped(ADDRESS));
    assertThat(pool.channels).containsOnly(channel3, channel4);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_grow_outside_of_reconnection() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int poolSize = 2;

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            // growth attempt
            .success(ADDRESS, channel3)
            .success(ADDRESS, channel4)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(ADDRESS));

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    pool.resize(4);
    waitForPendingAdminTasks();

    // The resizing should have triggered a reconnection
    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(ADDRESS));

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(ADDRESS));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStopped(ADDRESS));

    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_grow_during_reconnection() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int poolSize = 2;

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    DriverChannel channel3 = newMockDriverChannel(3);
    CompletableFuture<DriverChannel> channel3Future = new CompletableFuture<>();
    DriverChannel channel4 = newMockDriverChannel(4);
    CompletableFuture<DriverChannel> channel4Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(ADDRESS, channel1)
            .failure(ADDRESS, "mock channel init failure")
            // first reconnection attempt
            .pending(ADDRESS, channel2Future)
            // extra reconnection attempt after we realize the pool must grow
            .pending(ADDRESS, channel3Future)
            .pending(ADDRESS, channel4Future)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS));

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1);

    // A reconnection should have been scheduled to add the missing channel, don't complete yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(ADDRESS));

    pool.resize(4);

    waitForPendingAdminTasks();

    // Complete the channel for the first reconnection, bringing the count to 2
    channel2Future.complete(channel2);
    factoryHelper.waitForCall(ADDRESS);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS));

    assertThat(pool.channels).containsOnly(channel1, channel2);

    // A second attempt should have been scheduled since we're now still under the target size
    Mockito.verify(reconnectionSchedule, times(2)).nextDelay();
    // Same reconnection is still running, no additional events
    inOrder.verify(eventBus, never()).fire(ChannelEvent.reconnectionStopped(ADDRESS));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.reconnectionStarted(ADDRESS));

    // Two more channels get opened, bringing us to the target count
    factoryHelper.waitForCalls(ADDRESS, 2);
    channel3Future.complete(channel3);
    channel4Future.complete(channel4);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(ADDRESS));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStopped(ADDRESS));

    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_switch_keyspace_on_existing_channels() throws Exception {
    int poolSize = 2;

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .build();

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    CqlIdentifier newKeyspace = CqlIdentifier.fromCql("new_keyspace");
    CompletionStage<Void> setKeyspaceFuture = pool.setKeyspace(newKeyspace);
    waitForPendingAdminTasks();

    Mockito.verify(channel1).setKeyspace(newKeyspace);
    Mockito.verify(channel2).setKeyspace(newKeyspace);

    assertThat(setKeyspaceFuture).isSuccess();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_switch_keyspace_on_pending_channels() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int poolSize = 2;

    DriverChannel channel1 = newMockDriverChannel(1);
    CompletableFuture<DriverChannel> channel1Future = new CompletableFuture<>();
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .failure(ADDRESS, "mock channel init failure")
            .failure(ADDRESS, "mock channel init failure")
            // reconnection
            .pending(ADDRESS, channel1Future)
            .pending(ADDRESS, channel2Future)
            .build();

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // Check that reconnection has kicked in, but do not complete it yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    Mockito.verify(eventBus).fire(ChannelEvent.reconnectionStarted(ADDRESS));
    factoryHelper.waitForCalls(ADDRESS, 2);

    // Switch keyspace, it succeeds immediately since there is no active channel
    CqlIdentifier newKeyspace = CqlIdentifier.fromCql("new_keyspace");
    CompletionStage<Void> setKeyspaceFuture = pool.setKeyspace(newKeyspace);
    waitForPendingAdminTasks();
    assertThat(setKeyspaceFuture).isSuccess();

    // Now let the two channels succeed to complete the reconnection
    channel1Future.complete(channel1);
    channel2Future.complete(channel2);
    waitForPendingAdminTasks();

    Mockito.verify(eventBus).fire(ChannelEvent.reconnectionStopped(ADDRESS));
    Mockito.verify(channel1).setKeyspace(newKeyspace);
    Mockito.verify(channel2).setKeyspace(newKeyspace);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_close_all_channels_when_closed() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int poolSize = 3;

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel3Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .failure(ADDRESS, "mock channel init failure")
            // reconnection
            .pending(ADDRESS, channel3Future)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(ADDRESS));

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // Reconnection should have kicked in and started to open a channel, do not complete it yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    factoryHelper.waitForCalls(ADDRESS, 1);

    CompletionStage<ChannelPool> closeFuture = pool.close();
    waitForPendingAdminTasks();

    // The two original channels were closed normally
    Mockito.verify(channel1).close();
    Mockito.verify(channel2).close();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelClosed(ADDRESS));

    // Complete the reconnecting channel
    channel3Future.complete(channel3);
    waitForPendingAdminTasks();

    // It should be force-closed once we find out the pool was closed
    Mockito.verify(channel3).forceClose();
    // No events because the channel was never really associated to the pool
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelOpened(ADDRESS));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelClosed(ADDRESS));

    // Note that we don't wait for reconnected channels to close, so the pool only depends on
    // channel 1 and 2
    ((ChannelPromise) channel1.closeFuture()).setSuccess();
    ((ChannelPromise) channel2.closeFuture()).setSuccess();

    assertThat(closeFuture).isSuccess();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_force_close_all_channels_when_force_closed() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    int poolSize = 3;

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel3Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .failure(ADDRESS, "mock channel init failure")
            // reconnection
            .pending(ADDRESS, channel3Future)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture = ChannelPool.init(ADDRESS, null, poolSize, context);

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(ADDRESS));

    // Reconnection should have kicked in and started to open a channel, do not complete it yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    factoryHelper.waitForCalls(ADDRESS, 1);

    CompletionStage<ChannelPool> closeFuture = pool.forceClose();
    waitForPendingAdminTasks();

    // The two original channels were force-closed
    Mockito.verify(channel1).close();
    Mockito.verify(channel2).close();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelClosed(ADDRESS));

    // Complete the reconnecting channel
    channel3Future.complete(channel3);
    waitForPendingAdminTasks();

    // It should be force-closed once we find out the pool was closed
    Mockito.verify(channel3).forceClose();
    // No events because the channel was never really associated to the pool
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelOpened(ADDRESS));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelClosed(ADDRESS));

    // Note that we don't wait for reconnected channels to close, so the pool only depends on
    // channel 1 and 2
    ((ChannelPromise) channel1.closeFuture()).setSuccess();
    ((ChannelPromise) channel2.closeFuture()).setSuccess();

    assertThat(closeFuture).isSuccess();

    factoryHelper.verifyNoMoreCalls();
  }

  private DriverChannel newMockDriverChannel(int id) {
    DriverChannel channel = Mockito.mock(DriverChannel.class);
    EventLoop adminExecutor = adminEventLoopGroup.next();
    DefaultChannelPromise closeFuture = new DefaultChannelPromise(null, adminExecutor);
    Mockito.when(channel.close()).thenReturn(closeFuture);
    Mockito.when(channel.forceClose()).thenReturn(closeFuture);
    Mockito.when(channel.closeFuture()).thenReturn(closeFuture);
    Mockito.when(channel.setKeyspace(any(CqlIdentifier.class)))
        .thenReturn(adminExecutor.newSucceededFuture(null));
    Mockito.when(channel.toString()).thenReturn("channel" + id);
    return channel;
  }

  // Wait for all the tasks on the pool's admin executor to complete.
  private void waitForPendingAdminTasks() {
    // This works because the event loop group is single-threaded
    Future<?> f = adminEventLoopGroup.schedule(() -> null, 5, TimeUnit.NANOSECONDS);
    try {
      Uninterruptibles.getUninterruptibly(f, 100, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      fail("unexpected error", e.getCause());
    } catch (TimeoutException e) {
      fail("timed out while waiting for admin tasks to complete", e);
    }
  }
}
