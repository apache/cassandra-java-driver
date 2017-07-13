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
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

public class ChannelPoolResizeTest extends ChannelPoolTestBase {

  @Test
  public void should_shrink_outside_of_reconnection() throws Exception {
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_REMOTE_CONNECTIONS)).thenReturn(4);
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(2);

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

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.REMOTE, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 4);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);
    inOrder.verify(eventBus, times(4)).fire(ChannelEvent.channelOpened(NODE));

    pool.resize(NodeDistance.LOCAL);

    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelClosed(NODE));

    assertThat(pool.channels).containsOnly(channel3, channel4);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_shrink_during_reconnection() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_REMOTE_CONNECTIONS)).thenReturn(4);
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(2);

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

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.REMOTE, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 4);
    waitForPendingAdminTasks();

    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));
    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    // A reconnection should have been scheduled to add the missing channels, don't complete yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(NODE));

    pool.resize(NodeDistance.LOCAL);

    waitForPendingAdminTasks();

    // Now allow the reconnected channels to complete initialization
    channel3Future.complete(channel3);
    channel4Future.complete(channel4);

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    // Pool should have shrinked back to 2. We keep the most recent channels so 1 and 2 get closed.
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelClosed(NODE));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStopped(NODE));
    assertThat(pool.channels).containsOnly(channel3, channel4);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_grow_outside_of_reconnection() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(2);
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_REMOTE_CONNECTIONS)).thenReturn(4);

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

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    pool.resize(NodeDistance.REMOTE);
    waitForPendingAdminTasks();

    // The resizing should have triggered a reconnection
    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(NODE));

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStopped(NODE));

    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_grow_during_reconnection() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(2);
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_REMOTE_CONNECTIONS)).thenReturn(4);

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

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(NODE));

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1);

    // A reconnection should have been scheduled to add the missing channel, don't complete yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(NODE));

    pool.resize(NodeDistance.REMOTE);

    waitForPendingAdminTasks();

    // Complete the channel for the first reconnection, bringing the count to 2
    channel2Future.complete(channel2);
    factoryHelper.waitForCall(ADDRESS);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(NODE));

    assertThat(pool.channels).containsOnly(channel1, channel2);

    // A second attempt should have been scheduled since we're now still under the target size
    Mockito.verify(reconnectionSchedule, times(2)).nextDelay();
    // Same reconnection is still running, no additional events
    inOrder.verify(eventBus, never()).fire(ChannelEvent.reconnectionStopped(NODE));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.reconnectionStarted(NODE));

    // Two more channels get opened, bringing us to the target count
    factoryHelper.waitForCalls(ADDRESS, 2);
    channel3Future.complete(channel3);
    channel4Future.complete(channel4);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStopped(NODE));

    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_resize_outside_of_reconnection_if_config_changes() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(2);

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

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    // Simulate a configuration change
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(4);
    eventBus.fire(ConfigChangeEvent.INSTANCE);
    waitForPendingAdminTasks();

    // It should have triggered a reconnection
    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(NODE));

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStopped(NODE));

    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_resize_during_reconnection_if_config_changes() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(2);

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

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(NODE));

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1);

    // A reconnection should have been scheduled to add the missing channel, don't complete yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(NODE));

    // Simulate a configuration change
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(4);
    eventBus.fire(ConfigChangeEvent.INSTANCE);
    waitForPendingAdminTasks();

    // Complete the channel for the first reconnection, bringing the count to 2
    channel2Future.complete(channel2);
    factoryHelper.waitForCall(ADDRESS);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(NODE));

    assertThat(pool.channels).containsOnly(channel1, channel2);

    // A second attempt should have been scheduled since we're now still under the target size
    Mockito.verify(reconnectionSchedule, times(2)).nextDelay();
    // Same reconnection is still running, no additional events
    inOrder.verify(eventBus, never()).fire(ChannelEvent.reconnectionStopped(NODE));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.reconnectionStarted(NODE));

    // Two more channels get opened, bringing us to the target count
    factoryHelper.waitForCalls(ADDRESS, 2);
    channel3Future.complete(channel3);
    channel4Future.complete(channel4);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStopped(NODE));

    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_ignore_config_change_if_not_relevant() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .build();
    InOrder inOrder = Mockito.inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus, times(2)).fire(ChannelEvent.channelOpened(NODE));

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    // Config changes, but not for our distance
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_REMOTE_CONNECTIONS)).thenReturn(1);
    eventBus.fire(ConfigChangeEvent.INSTANCE);
    waitForPendingAdminTasks();

    // It should not have triggered a reconnection
    Mockito.verify(reconnectionSchedule, never()).nextDelay();

    factoryHelper.verifyNoMoreCalls();
  }
}
