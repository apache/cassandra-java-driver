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

import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.ClusterNameMismatchException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

public class ChannelPoolInitTest extends ChannelPoolTestBase {

  @Test
  public void should_initialize_when_all_channels_succeed() throws Exception {
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(3);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .success(ADDRESS, channel3)
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();

    assertThat(poolFuture)
        .isSuccess(pool -> assertThat(pool.channels).containsOnly(channel1, channel2, channel3));
    Mockito.verify(eventBus, times(3)).fire(ChannelEvent.channelOpened(NODE));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_initialize_when_all_channels_fail() throws Exception {
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(3);

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(ADDRESS, "mock channel init failure")
            .failure(ADDRESS, "mock channel init failure")
            .failure(ADDRESS, "mock channel init failure")
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess(pool -> assertThat(pool.channels).isEmpty());
    Mockito.verify(eventBus, never()).fire(ChannelEvent.channelOpened(NODE));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_indicate_when_keyspace_failed_on_all_channels() {
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(3);

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(ADDRESS, new InvalidKeyspaceException("invalid keyspace"))
            .failure(ADDRESS, new InvalidKeyspaceException("invalid keyspace"))
            .failure(ADDRESS, new InvalidKeyspaceException("invalid keyspace"))
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();
    assertThat(poolFuture).isSuccess(pool -> assertThat(pool.isInvalidKeyspace()).isTrue());
  }

  @Test
  public void should_fire_force_down_event_when_cluster_name_does_not_match() throws Exception {
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(3);

    ClusterNameMismatchException error =
        new ClusterNameMismatchException(ADDRESS, "actual", "expected");
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(ADDRESS, error)
            .failure(ADDRESS, error)
            .failure(ADDRESS, error)
            .build();

    ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 3);
    waitForPendingAdminTasks();

    Mockito.verify(eventBus).fire(TopologyEvent.forceDown(ADDRESS));
    Mockito.verify(eventBus, never()).fire(ChannelEvent.channelOpened(NODE));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_when_init_incomplete() throws Exception {
    // Short delay so we don't have to wait in the test
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(2);

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

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1);
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(NODE));

    // A reconnection should have been scheduled
    Mockito.verify(reconnectionSchedule).nextDelay();
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStarted(NODE));

    channel2Future.complete(channel2);
    factoryHelper.waitForCalls(ADDRESS, 1);
    waitForPendingAdminTasks();
    inOrder.verify(eventBus).fire(ChannelEvent.channelOpened(NODE));
    inOrder.verify(eventBus).fire(ChannelEvent.reconnectionStopped(NODE));

    assertThat(pool.channels).containsOnly(channel1, channel2);

    factoryHelper.verifyNoMoreCalls();
  }
}
