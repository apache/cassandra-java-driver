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
package com.datastax.oss.driver.internal.core.control;

import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;

public class ControlConnectionTest extends ControlConnectionTestBase {

  @Test
  public void should_close_successfully_if_it_was_never_init() {
    // When
    CompletionStage<Void> closeFuture = controlConnection.forceClose();

    // Then
    assertThat(closeFuture).isSuccess();
  }

  @Test
  public void should_init_with_first_contact_point_if_reachable() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(ADDRESS1, channel1).build();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);
    waitForPendingAdminTasks();

    // Then
    assertThat(initFuture).isSuccess();
    assertThat(controlConnection.channel()).isEqualTo(channel1);
    Mockito.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS1));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_always_return_same_init_future() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(ADDRESS1, channel1).build();

    // When
    CompletionStage<Void> initFuture1 = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);
    CompletionStage<Void> initFuture2 = controlConnection.init(false);

    // Then
    assertThat(initFuture1).isEqualTo(initFuture2);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_init_with_second_contact_point_if_first_one_fails() {
    // Given
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(ADDRESS1, "mock failure")
            .success(ADDRESS2, channel2)
            .build();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);
    factoryHelper.waitForCall(ADDRESS2);
    waitForPendingAdminTasks();

    // Then
    assertThat(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    Mockito.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS2));
    // each attempt tries all nodes, so there is no reconnection
    Mockito.verify(reconnectionPolicy, never()).newSchedule();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_fail_to_init_if_all_contact_points_fail() {
    // Given
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(ADDRESS1, "mock failure")
            .failure(ADDRESS2, "mock failure")
            .build();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);
    factoryHelper.waitForCall(ADDRESS2);
    waitForPendingAdminTasks();

    // Then
    assertThat(initFuture).isFailed();
    Mockito.verify(eventBus, never()).fire(any(ChannelEvent.class));
    // no reconnections at init
    Mockito.verify(reconnectionPolicy, never()).newSchedule();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_if_channel_goes_down() throws Exception {
    // Given
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS1, channel1)
            .failure(ADDRESS1, "mock failure")
            .success(ADDRESS2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);

    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    assertThat(controlConnection.channel()).isEqualTo(channel1);
    Mockito.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS1));

    // When
    failChannel(channel1, "mock channel failure");
    waitForPendingAdminTasks();

    // Then
    // a reconnection was started
    Mockito.verify(reconnectionSchedule).nextDelay();
    factoryHelper.waitForCall(ADDRESS1);
    factoryHelper.waitForCall(ADDRESS2);
    waitForPendingAdminTasks();
    assertThat(controlConnection.channel()).isEqualTo(channel2);
    Mockito.verify(eventBus).fire(ChannelEvent.channelClosed(ADDRESS1));
    Mockito.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS2));
    Mockito.verify(metadataManager).refreshNodes();
    Mockito.verify(loadBalancingPolicyWrapper).init();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_force_reconnection_if_pending() {
    // Given
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS1, channel1)
            .failure(ADDRESS1, "mock failure")
            .success(ADDRESS2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    assertThat(controlConnection.channel()).isEqualTo(channel1);
    Mockito.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS1));

    // the channel fails and a reconnection is scheduled for later
    failChannel(channel1, "mock channel failure");
    waitForPendingAdminTasks();
    Mockito.verify(eventBus).fire(ChannelEvent.channelClosed(ADDRESS1));
    Mockito.verify(reconnectionSchedule).nextDelay();

    // When
    controlConnection.reconnectNow();
    factoryHelper.waitForCall(ADDRESS1);
    factoryHelper.waitForCall(ADDRESS2);
    waitForPendingAdminTasks();

    // Then
    assertThat(controlConnection.channel()).isEqualTo(channel2);
    Mockito.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_force_reconnection_even_if_connected() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS1, channel1)
            .failure(ADDRESS1, "mock failure")
            .success(ADDRESS2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    assertThat(controlConnection.channel()).isEqualTo(channel1);
    Mockito.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS1));

    // When
    controlConnection.reconnectNow();

    // Then
    factoryHelper.waitForCall(ADDRESS1);
    factoryHelper.waitForCall(ADDRESS2);
    waitForPendingAdminTasks();
    assertThat(controlConnection.channel()).isEqualTo(channel2);
    Mockito.verify(channel1).forceClose();
    Mockito.verify(eventBus).fire(ChannelEvent.channelClosed(ADDRESS1));
    Mockito.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_not_force_reconnection_if_not_init() {
    // When
    controlConnection.reconnectNow();
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(reconnectionSchedule, never()).nextDelay();
  }

  @Test
  public void should_not_force_reconnection_if_closed() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(ADDRESS1, channel1).build();
    CompletionStage<Void> initFuture = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    CompletionStage<Void> closeFuture = controlConnection.forceClose();
    assertThat(closeFuture).isSuccess();

    // When
    controlConnection.reconnectNow();
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(reconnectionSchedule, never()).nextDelay();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_close_channel_when_closing() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(ADDRESS1, channel1).build();

    CompletionStage<Void> initFuture = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();

    // When
    CompletionStage<Void> closeFuture = controlConnection.forceClose();
    waitForPendingAdminTasks();

    // Then
    assertThat(closeFuture).isSuccess();
    Mockito.verify(channel1).forceClose();
    Mockito.verify(eventBus).fire(ChannelEvent.channelClosed(ADDRESS1));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_close_channel_if_closed_during_reconnection() {
    // Given
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS1, channel1)
            .failure(ADDRESS1, "mock failure")
            .pending(ADDRESS2, channel2Future)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    assertThat(controlConnection.channel()).isEqualTo(channel1);
    Mockito.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS1));

    // the channel fails and a reconnection is scheduled
    failChannel(channel1, "mock channel failure");
    waitForPendingAdminTasks();
    Mockito.verify(eventBus).fire(ChannelEvent.channelClosed(ADDRESS1));
    Mockito.verify(reconnectionSchedule).nextDelay();
    factoryHelper.waitForCall(ADDRESS1);
    // channel2 starts initializing (but the future is not completed yet)
    factoryHelper.waitForCall(ADDRESS2);

    // When
    // the control connection gets closed before channel2 initialization is complete
    controlConnection.forceClose();
    waitForPendingAdminTasks();
    channel2Future.complete(channel2);
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(channel2).forceClose();
    // no event because the control connection never "owned" the channel
    Mockito.verify(eventBus, never()).fire(ChannelEvent.channelOpened(ADDRESS2));
    Mockito.verify(eventBus, never()).fire(ChannelEvent.channelClosed(ADDRESS2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_handle_channel_failure_if_closed_during_reconnection() {
    // Given
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel1Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS1, channel1)
            .pending(ADDRESS1, channel1Future)
            .success(ADDRESS2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false);
    factoryHelper.waitForCall(ADDRESS1);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    assertThat(controlConnection.channel()).isEqualTo(channel1);
    Mockito.verify(eventBus).fire(ChannelEvent.channelOpened(ADDRESS1));

    // the channel fails and a reconnection is scheduled
    failChannel(channel1, "mock channel failure");
    waitForPendingAdminTasks();
    Mockito.verify(eventBus).fire(ChannelEvent.channelClosed(ADDRESS1));
    Mockito.verify(reconnectionSchedule).nextDelay();
    // channel1 starts initializing (but the future is not completed yet)
    factoryHelper.waitForCall(ADDRESS1);

    // When
    // the control connection gets closed before channel1 initialization fails
    controlConnection.forceClose();
    channel1Future.completeExceptionally(new Exception("mock failure"));
    waitForPendingAdminTasks();

    // Then
    // should never try channel2 because the reconnection has detected that it can stop after the
    // first failure
    factoryHelper.verifyNoMoreCalls();
  }
}
