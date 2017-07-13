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
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

public class ChannelPoolShutdownTest extends ChannelPoolTestBase {

  @Test
  public void should_close_all_channels_when_closed() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(3);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);
    CompletableFuture<DriverChannel> channel4Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .success(ADDRESS, channel3)
            // reconnection
            .pending(ADDRESS, channel4Future)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(3)).fire(ChannelEvent.channelOpened(NODE));

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // Simulate graceful shutdown on channel3
    ((ChannelPromise) channel3.closeStartedFuture()).setSuccess();
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(1)).fire(ChannelEvent.channelClosed(NODE));

    // Reconnection should have kicked in and started to open channel4, do not complete it yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    factoryHelper.waitForCalls(ADDRESS, 1);

    CompletionStage<Void> closeFuture = pool.closeAsync();
    waitForPendingAdminTasks();

    // The two original channels were closed normally
    Mockito.verify(channel1).close();
    Mockito.verify(channel2).close();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelClosed(NODE));
    // The closing channel was not closed again
    Mockito.verify(channel3, never()).close();

    // Complete the reconnecting channel
    channel4Future.complete(channel4);
    waitForPendingAdminTasks();

    // It should be force-closed once we find out the pool was closed
    Mockito.verify(channel4).forceClose();
    // No events because the channel was never really associated to the pool
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelOpened(NODE));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelClosed(NODE));

    // We don't wait for reconnected channels to close, so the pool only depends on channel 1 to 3
    ((ChannelPromise) channel1.closeFuture()).setSuccess();
    ((ChannelPromise) channel2.closeFuture()).setSuccess();
    ((ChannelPromise) channel3.closeFuture()).setSuccess();

    assertThat(closeFuture).isSuccess();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_force_close_all_channels_when_force_closed() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(3);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);
    CompletableFuture<DriverChannel> channel4Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .success(ADDRESS, channel3)
            // reconnection
            .pending(ADDRESS, channel4Future)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    inOrder.verify(eventBus, times(3)).fire(ChannelEvent.channelOpened(NODE));

    // Simulate graceful shutdown on channel3
    ((ChannelPromise) channel3.closeStartedFuture()).setSuccess();
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(1)).fire(ChannelEvent.channelClosed(NODE));

    // Reconnection should have kicked in and started to open a channel, do not complete it yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    factoryHelper.waitForCalls(ADDRESS, 1);

    CompletionStage<Void> closeFuture = pool.forceCloseAsync();
    waitForPendingAdminTasks();

    // The three original channels were force-closed
    Mockito.verify(channel1).forceClose();
    Mockito.verify(channel2).forceClose();
    Mockito.verify(channel3).forceClose();
    // Only two events because the one for channel3 was sent earlier
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelClosed(NODE));

    // Complete the reconnecting channel
    channel4Future.complete(channel4);
    waitForPendingAdminTasks();

    // It should be force-closed once we find out the pool was closed
    Mockito.verify(channel4).forceClose();
    // No events because the channel was never really associated to the pool
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelOpened(NODE));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelClosed(NODE));

    // We don't wait for reconnected channels to close, so the pool only depends on channel 1-3
    ((ChannelPromise) channel1.closeFuture()).setSuccess();
    ((ChannelPromise) channel2.closeFuture()).setSuccess();
    ((ChannelPromise) channel3.closeFuture()).setSuccess();

    assertThat(closeFuture).isSuccess();

    factoryHelper.verifyNoMoreCalls();
  }
}
