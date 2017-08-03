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

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import io.netty.channel.ChannelPromise;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

public class ChannelPoolReconnectTest extends ChannelPoolTestBase {

  @Test
  public void should_reconnect_when_channel_closes() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

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

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));

    // Simulate fatal error on channel2
    ((ChannelPromise) channel2.closeFuture())
        .setFailure(new Exception("mock channel init failure"));
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelClosed(NODE));

    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(NODE));
    factoryHelper.waitForCall(ADDRESS);

    channel3Future.complete(channel3);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(NODE));
    Mockito.verify(eventBus).fire(ChannelEvent.reconnectionStopped(NODE));

    assertThat(pool.channels).containsOnly(channel1, channel3);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_when_channel_starts_graceful_shutdown() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

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

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));

    // Simulate graceful shutdown on channel2
    ((ChannelPromise) channel2.closeStartedFuture()).setSuccess();
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelClosed(NODE));

    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(NODE));
    factoryHelper.waitForCall(ADDRESS);

    channel3Future.complete(channel3);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(NODE));
    Mockito.verify(eventBus).fire(ChannelEvent.reconnectionStopped(NODE));

    assertThat(pool.channels).containsOnly(channel1, channel3);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_let_current_attempt_complete_when_reconnecting_now()
      throws ExecutionException, InterruptedException {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(1);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(ADDRESS, channel1)
            // reconnection
            .pending(ADDRESS, channel2Future)
            .build();

    InOrder inOrder = Mockito.inOrder(eventBus);

    // Initial connection
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");
    factoryHelper.waitForCalls(ADDRESS, 1);
    waitForPendingAdminTasks();
    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    inOrder.verify(eventBus, times(1)).fire(ChannelEvent.channelOpened(NODE));

    // Kill channel1, reconnection begins and starts initializing channel2, but the initialization
    // is still pending (channel2Future not completed)
    ((ChannelPromise) channel1.closeStartedFuture()).setSuccess();
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelClosed(NODE));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(NODE));
    Mockito.verify(reconnectionSchedule).nextDelay();
    factoryHelper.waitForCalls(ADDRESS, 1);

    // Force a reconnection, should not try to create a new channel since we have a pending one
    pool.reconnectNow();
    waitForPendingAdminTasks();
    factoryHelper.verifyNoMoreCalls();
    inOrder.verify(eventBus, never()).fire(any());

    // Complete the initialization of channel2, reconnection succeeds
    channel2Future.complete(channel2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(NODE));
    Mockito.verify(eventBus).fire(ChannelEvent.reconnectionStopped(NODE));

    assertThat(pool.channels).containsOnly(channel2);

    factoryHelper.verifyNoMoreCalls();
  }
}
