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
package com.datastax.oss.driver.internal.core.control;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class ControlConnectionTest extends ControlConnectionTestBase {

  @Test
  public void should_close_successfully_if_it_was_never_init() {
    // When
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();

    // Then
    assertThatStage(closeFuture).isSuccess();
  }

  @Test
  public void should_init_with_first_contact_point_if_reachable() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    // Then
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_always_return_same_init_future() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    // When
    CompletionStage<Void> initFuture1 = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    CompletionStage<Void> initFuture2 = controlConnection.init(false, false, false);

    // Then
    assertThatStage(initFuture1).isEqualTo(initFuture2);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_init_with_second_contact_point_if_first_one_fails() {
    // Given
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node1, "mock failure")
            .success(node2, channel2)
            .build();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);

    // Then
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.controlConnectionFailed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));
    // each attempt tries all nodes, so there is no reconnection
    verify(reconnectionPolicy, never()).newNodeSchedule(any(Node.class));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_fail_to_init_if_all_contact_points_fail() {
    // Given
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node1, "mock failure")
            .failure(node2, "mock failure")
            .build();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);

    // Then
    assertThatStage(initFuture).isFailed();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.controlConnectionFailed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.controlConnectionFailed(node2));
    // no reconnections at init
    verify(reconnectionPolicy, never()).newNodeSchedule(any(Node.class));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_if_channel_goes_down() throws Exception {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .failure(node1, "mock failure")
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // When
    channel1.close();

    // Then
    // a reconnection was started
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));
    verify(metadataManager, VERIFY_TIMEOUT).refreshNodes();
    verify(loadBalancingPolicyWrapper, VERIFY_TIMEOUT).init();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_if_node_becomes_ignored() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // When
    mockQueryPlan(node2);
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node1));

    // Then
    // an immediate reconnection was started
    factoryHelper.waitForCall(node2);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(reconnectionSchedule, never()).nextDelay();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));
    verify(metadataManager, VERIFY_TIMEOUT).refreshNodes();
    verify(loadBalancingPolicyWrapper, VERIFY_TIMEOUT).init();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_if_node_is_removed() {
    should_reconnect_if_event(NodeStateEvent.removed(node1));
  }

  @Test
  public void should_reconnect_if_node_is_forced_down() {
    should_reconnect_if_event(NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, node1));
  }

  private void should_reconnect_if_event(NodeStateEvent event) {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // When
    mockQueryPlan(node2);
    eventBus.fire(event);

    // Then
    // an immediate reconnection was started
    factoryHelper.waitForCall(node2);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(reconnectionSchedule, never()).nextDelay();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));
    verify(metadataManager, VERIFY_TIMEOUT).refreshNodes();
    verify(loadBalancingPolicyWrapper, VERIFY_TIMEOUT).init();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_if_node_became_ignored_during_reconnection_attempt() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    DriverChannel channel3 = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node1, channel1)
            // reconnection
            .pending(node2, channel2Future)
            .success(node1, channel3)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    mockQueryPlan(node2, node1);
    // channel1 goes down, triggering a reconnection
    channel1.close();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    // the reconnection to node2 is in progress
    factoryHelper.waitForCall(node2);

    // When
    // node2 becomes ignored
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));
    // the reconnection to node2 completes
    channel2Future.complete(channel2);

    // Then
    // The channel should get closed and we should try the next node
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    factoryHelper.waitForCall(node1);
  }

  @Test
  public void should_reconnect_if_node_was_removed_during_reconnection_attempt() {
    should_reconnect_if_event_during_reconnection_attempt(NodeStateEvent.removed(node2));
  }

  @Test
  public void should_reconnect_if_node_was_forced_down_during_reconnection_attempt() {
    should_reconnect_if_event_during_reconnection_attempt(
        NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, node2));
  }

  private void should_reconnect_if_event_during_reconnection_attempt(NodeStateEvent event) {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    DriverChannel channel3 = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node1, channel1)
            // reconnection
            .pending(node2, channel2Future)
            .success(node1, channel3)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    assertThatStage(initFuture).isSuccess();
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    mockQueryPlan(node2, node1);
    // channel1 goes down, triggering a reconnection
    channel1.close();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    // the reconnection to node2 is in progress
    factoryHelper.waitForCall(node2);

    // When
    // node2 goes into the new state
    eventBus.fire(event);
    // the reconnection to node2 completes
    channel2Future.complete(channel2);

    // Then
    // The channel should get closed and we should try the next node
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    factoryHelper.waitForCall(node1);
  }

  @Test
  public void should_force_reconnection_if_pending() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .failure(node1, "mock failure")
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // the channel fails and a reconnection is scheduled for later
    channel1.close();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();

    // When
    controlConnection.reconnectNow();
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);

    // Then
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_force_reconnection_even_if_connected() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .failure(node1, "mock failure")
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // When
    controlConnection.reconnectNow();

    // Then
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(channel1, VERIFY_TIMEOUT).forceClose();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_not_force_reconnection_if_not_init() throws InterruptedException {
    // When
    controlConnection.reconnectNow();
    TimeUnit.MILLISECONDS.sleep(500);

    // Then
    verify(reconnectionSchedule, never()).nextDelay();
  }

  @Test
  public void should_not_force_reconnection_if_closed() throws InterruptedException {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();
    assertThatStage(closeFuture).isSuccess();

    // When
    controlConnection.reconnectNow();
    TimeUnit.MILLISECONDS.sleep(500);

    // Then
    verify(reconnectionSchedule, never()).nextDelay();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_close_channel_when_closing() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    // When
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();

    // Then
    assertThatStage(closeFuture).isSuccess();
    verify(channel1, VERIFY_TIMEOUT).forceClose();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_close_channel_if_closed_during_reconnection() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .failure(node1, "mock failure")
            .pending(node2, channel2Future)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // the channel fails and a reconnection is scheduled
    channel1.close();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCall(node1);
    // channel2 starts initializing (but the future is not completed yet)
    factoryHelper.waitForCall(node2);

    // When
    // the control connection gets closed before channel2 initialization is complete
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();
    assertThatStage(closeFuture).isSuccess();
    channel2Future.complete(channel2);

    // Then
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    // no event because the control connection never "owned" the channel
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(node2));
    verify(eventBus, never()).fire(ChannelEvent.channelClosed(node2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_handle_channel_failure_if_closed_during_reconnection() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel1Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .pending(node1, channel1Future)
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // the channel fails and a reconnection is scheduled
    channel1.close();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    // channel1 starts initializing (but the future is not completed yet)
    factoryHelper.waitForCall(node1);

    // When
    // the control connection gets closed before channel1 initialization fails
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();
    assertThatStage(closeFuture).isSuccess();
    channel1Future.completeExceptionally(new Exception("mock failure"));

    // Then
    // should never try channel2 because the reconnection has detected that it can stop after the
    // first failure
    factoryHelper.verifyNoMoreCalls();
  }
}
