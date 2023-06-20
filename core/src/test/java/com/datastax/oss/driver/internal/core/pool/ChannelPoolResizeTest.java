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
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.InOrder;

public class ChannelPoolResizeTest extends ChannelPoolTestBase {

  @Test
  public void should_shrink_outside_of_reconnection() throws Exception {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE)).thenReturn(4);
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node, channel1)
            .success(node, channel2)
            .success(node, channel3)
            .success(node, channel4)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.REMOTE, context, "test");

    factoryHelper.waitForCalls(node, 4);

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4);
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(4)).fire(ChannelEvent.channelOpened(node));

    pool.resize(NodeDistance.LOCAL);

    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelClosed(node));

    await().untilAsserted(() -> assertThat(pool.channels).containsOnly(channel3, channel4));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_shrink_during_reconnection() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE)).thenReturn(4);
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    CompletableFuture<DriverChannel> channel3Future = new CompletableFuture<>();
    DriverChannel channel4 = newMockDriverChannel(4);
    CompletableFuture<DriverChannel> channel4Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node, channel1)
            .success(node, channel2)
            .failure(node, "mock channel init failure")
            .failure(node, "mock channel init failure")
            // reconnection
            .pending(node, channel3Future)
            .pending(node, channel4Future)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.REMOTE, context, "test");

    factoryHelper.waitForCalls(node, 4);

    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelOpened(node));
    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    // A reconnection should have been scheduled to add the missing channels, don't complete yet
    verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(node));

    pool.resize(NodeDistance.LOCAL);

    TimeUnit.MILLISECONDS.sleep(200);

    // Now allow the reconnected channels to complete initialization
    channel3Future.complete(channel3);
    channel4Future.complete(channel4);

    factoryHelper.waitForCalls(node, 2);

    // Pool should have shrunk back to 2. We keep the most recent channels so 1 and 2 get closed.
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelOpened(node));
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelClosed(node));
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStopped(node));
    await().untilAsserted(() -> assertThat(pool.channels).containsOnly(channel3, channel4));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_grow_outside_of_reconnection() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE)).thenReturn(4);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node, channel1)
            .success(node, channel2)
            // growth attempt
            .success(node, channel3)
            .success(node, channel4)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 2);
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelOpened(node));

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    pool.resize(NodeDistance.REMOTE);

    // The resizing should have triggered a reconnection
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStarted(node));

    factoryHelper.waitForCalls(node, 2);
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelOpened(node));
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStopped(node));

    await()
        .untilAsserted(
            () -> assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_grow_during_reconnection() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE)).thenReturn(4);

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
            .success(node, channel1)
            .failure(node, "mock channel init failure")
            // first reconnection attempt
            .pending(node, channel2Future)
            // extra reconnection attempt after we realize the pool must grow
            .pending(node, channel3Future)
            .pending(node, channel4Future)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 2);
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node));

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1);

    // A reconnection should have been scheduled to add the missing channel, don't complete yet
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStarted(node));

    pool.resize(NodeDistance.REMOTE);

    TimeUnit.MILLISECONDS.sleep(200);

    // Complete the channel for the first reconnection, bringing the count to 2
    channel2Future.complete(channel2);
    factoryHelper.waitForCall(node);
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node));

    await().untilAsserted(() -> assertThat(pool.channels).containsOnly(channel1, channel2));

    // A second attempt should have been scheduled since we're now still under the target size
    verify(reconnectionSchedule, VERIFY_TIMEOUT.times(2)).nextDelay();
    // Same reconnection is still running, no additional events
    inOrder.verify(eventBus, never()).fire(ChannelEvent.reconnectionStopped(node));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.reconnectionStarted(node));

    // Two more channels get opened, bringing us to the target count
    factoryHelper.waitForCalls(node, 2);
    channel3Future.complete(channel3);
    channel4Future.complete(channel4);
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelOpened(node));
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStopped(node));

    await()
        .untilAsserted(
            () -> assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_resize_outside_of_reconnection_if_config_changes() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node, channel1)
            .success(node, channel2)
            // growth attempt
            .success(node, channel3)
            .success(node, channel4)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 2);
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelOpened(node));

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    // Simulate a configuration change
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(4);
    eventBus.fire(ConfigChangeEvent.INSTANCE);

    // It should have triggered a reconnection
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStarted(node));

    factoryHelper.waitForCalls(node, 2);
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelOpened(node));
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStopped(node));

    await()
        .untilAsserted(
            () -> assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_resize_during_reconnection_if_config_changes() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

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
            .success(node, channel1)
            .failure(node, "mock channel init failure")
            // first reconnection attempt
            .pending(node, channel2Future)
            // extra reconnection attempt after we realize the pool must grow
            .pending(node, channel3Future)
            .pending(node, channel4Future)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 2);
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node));

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1);

    // A reconnection should have been scheduled to add the missing channel, don't complete yet
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStarted(node));

    // Simulate a configuration change
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(4);
    eventBus.fire(ConfigChangeEvent.INSTANCE);
    TimeUnit.MILLISECONDS.sleep(200);

    // Complete the channel for the first reconnection, bringing the count to 2
    channel2Future.complete(channel2);
    factoryHelper.waitForCall(node);
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node));

    await().untilAsserted(() -> assertThat(pool.channels).containsOnly(channel1, channel2));

    // A second attempt should have been scheduled since we're now still under the target size
    verify(reconnectionSchedule, VERIFY_TIMEOUT.times(2)).nextDelay();
    // Same reconnection is still running, no additional events
    inOrder.verify(eventBus, never()).fire(ChannelEvent.reconnectionStopped(node));
    inOrder.verify(eventBus, never()).fire(ChannelEvent.reconnectionStarted(node));

    // Two more channels get opened, bringing us to the target count
    factoryHelper.waitForCalls(node, 2);
    channel3Future.complete(channel3);
    channel4Future.complete(channel4);
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelOpened(node));
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStopped(node));

    await()
        .untilAsserted(
            () -> assertThat(pool.channels).containsOnly(channel1, channel2, channel3, channel4));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_ignore_config_change_if_not_relevant() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node, channel1)
            .success(node, channel2)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 2);
    inOrder.verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelOpened(node));

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    // Config changes, but not for our distance
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE)).thenReturn(1);
    eventBus.fire(ConfigChangeEvent.INSTANCE);
    TimeUnit.MILLISECONDS.sleep(200);

    // It should not have triggered a reconnection
    verify(reconnectionSchedule, never()).nextDelay();

    factoryHelper.verifyNoMoreCalls();
  }
}
