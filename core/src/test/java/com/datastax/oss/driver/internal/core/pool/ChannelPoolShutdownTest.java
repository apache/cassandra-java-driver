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
package com.datastax.oss.driver.internal.core.pool;

import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
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

public class ChannelPoolShutdownTest extends ChannelPoolTestBase {

  @Test
  public void should_close_all_channels_when_closed() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(3);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);
    CompletableFuture<DriverChannel> channel4Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node, channel1)
            .success(node, channel2)
            .success(node, channel3)
            // reconnection
            .pending(node, channel4Future)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 3);
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(3)).fire(ChannelEvent.channelOpened(node));

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // Simulate graceful shutdown on channel3
    ((ChannelPromise) channel3.closeStartedFuture()).setSuccess();
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(1)).fire(ChannelEvent.channelClosed(node));

    // Reconnection should have kicked in and started to open channel4, do not complete it yet
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCalls(node, 1);

    CompletionStage<Void> closeFuture = pool.closeAsync();

    // The two original channels were closed normally
    verify(channel1, VERIFY_TIMEOUT).close();
    verify(channel2, VERIFY_TIMEOUT).close();
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelClosed(node));
    // The closing channel was not closed again
    verify(channel3, never()).close();

    // Complete the reconnecting channel
    channel4Future.complete(channel4);

    // It should be force-closed once we find out the pool was closed
    verify(channel4, VERIFY_TIMEOUT).forceClose();
    // No events because the channel was never really associated to the pool
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelOpened(node));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelClosed(node));

    // We don't wait for reconnected channels to close, so the pool only depends on channel 1 to 3
    ((ChannelPromise) channel1.closeFuture()).setSuccess();
    ((ChannelPromise) channel2.closeFuture()).setSuccess();
    ((ChannelPromise) channel3.closeFuture()).setSuccess();

    assertThatStage(closeFuture).isSuccess();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_force_close_all_channels_when_force_closed() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(3);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);
    CompletableFuture<DriverChannel> channel4Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node, channel1)
            .success(node, channel2)
            .success(node, channel3)
            // reconnection
            .pending(node, channel4Future)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 3);

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(3)).fire(ChannelEvent.channelOpened(node));

    // Simulate graceful shutdown on channel3
    ((ChannelPromise) channel3.closeStartedFuture()).setSuccess();
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(1)).fire(ChannelEvent.channelClosed(node));

    // Reconnection should have kicked in and started to open a channel, do not complete it yet
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCalls(node, 1);

    CompletionStage<Void> closeFuture = pool.forceCloseAsync();

    // The three original channels were force-closed
    verify(channel1, VERIFY_TIMEOUT).forceClose();
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    verify(channel3, VERIFY_TIMEOUT).forceClose();
    // Only two events because the one for channel3 was sent earlier
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelClosed(node));

    // Complete the reconnecting channel
    channel4Future.complete(channel4);

    // It should be force-closed once we find out the pool was closed
    verify(channel4, VERIFY_TIMEOUT).forceClose();
    // No events because the channel was never really associated to the pool
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelOpened(node));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.channelClosed(node));

    // We don't wait for reconnected channels to close, so the pool only depends on channel 1-3
    ((ChannelPromise) channel1.closeFuture()).setSuccess();
    ((ChannelPromise) channel2.closeFuture()).setSuccess();
    ((ChannelPromise) channel3.closeFuture()).setSuccess();

    assertThatStage(closeFuture).isSuccess();

    factoryHelper.verifyNoMoreCalls();
  }
}
