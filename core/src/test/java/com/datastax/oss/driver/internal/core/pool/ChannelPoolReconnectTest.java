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
package com.datastax.oss.driver.internal.core.pool;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.mockito.InOrder;

public class ChannelPoolReconnectTest extends ChannelPoolTestBase {

  @Test
  public void should_reconnect_when_channel_closes() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    CompletableFuture<DriverChannel> channel3Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node, channel1)
            .success(node, channel2)
            // reconnection
            .pending(node, channel3Future)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 2);
    waitForPendingAdminTasks();

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(node));

    // Simulate fatal error on channel2
    ((ChannelPromise) channel2.closeFuture())
        .setFailure(new Exception("mock channel init failure"));
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelClosed(node));

    verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(node));
    factoryHelper.waitForCall(node);

    channel3Future.complete(channel3);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(node));
    verify(eventBus).fire(ChannelEvent.reconnectionStopped(node));

    assertThat(pool.channels).containsOnly(channel1, channel3);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_when_channel_starts_graceful_shutdown() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    CompletableFuture<DriverChannel> channel3Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node, channel1)
            .success(node, channel2)
            // reconnection
            .pending(node, channel3Future)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 2);
    waitForPendingAdminTasks();

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(node));

    // Simulate graceful shutdown on channel2
    ((ChannelPromise) channel2.closeStartedFuture()).setSuccess();
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelClosed(node));

    verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(node));
    factoryHelper.waitForCall(node);

    channel3Future.complete(channel3);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(node));
    verify(eventBus).fire(ChannelEvent.reconnectionStopped(node));

    assertThat(pool.channels).containsOnly(channel1, channel3);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_let_current_attempt_complete_when_reconnecting_now()
      throws ExecutionException, InterruptedException {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(1);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node, channel1)
            // reconnection
            .pending(node, channel2Future)
            .build();

    InOrder inOrder = inOrder(eventBus);

    // Initial connection
    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");
    factoryHelper.waitForCalls(node, 1);
    waitForPendingAdminTasks();
    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    inOrder.verify(eventBus, times(1)).fire(ChannelEvent.channelOpened(node));

    // Kill channel1, reconnection begins and starts initializing channel2, but the initialization
    // is still pending (channel2Future not completed)
    ((ChannelPromise) channel1.closeStartedFuture()).setSuccess();
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelClosed(node));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(node));
    verify(reconnectionSchedule).nextDelay();
    factoryHelper.waitForCalls(node, 1);

    // Force a reconnection, should not try to create a new channel since we have a pending one
    pool.reconnectNow();
    waitForPendingAdminTasks();
    factoryHelper.verifyNoMoreCalls();
    inOrder.verify(eventBus, never()).fire(any());

    // Complete the initialization of channel2, reconnection succeeds
    channel2Future.complete(channel2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(node));
    verify(eventBus).fire(ChannelEvent.reconnectionStopped(node));

    assertThat(pool.channels).containsOnly(channel2);

    factoryHelper.verifyNoMoreCalls();
  }
}
