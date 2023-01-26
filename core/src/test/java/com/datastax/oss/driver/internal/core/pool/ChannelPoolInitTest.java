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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.pool;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.ClusterNameMismatchException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Test;
import org.mockito.InOrder;

public class ChannelPoolInitTest extends ChannelPoolTestBase {

  @Test
  public void should_initialize_when_all_channels_succeed() throws Exception {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(3);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node, channel1)
            .success(node, channel2)
            .success(node, channel3)
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 3);

    assertThatStage(poolFuture)
        .isSuccess(pool -> assertThat(pool.channels[0]).containsOnly(channel1, channel2, channel3));
    verify(eventBus, VERIFY_TIMEOUT.times(3)).fire(ChannelEvent.channelOpened(node));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_initialize_when_all_channels_fail() throws Exception {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(3);

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node, "mock channel init failure")
            .failure(node, "mock channel init failure")
            .failure(node, "mock channel init failure")
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);

    assertThatStage(poolFuture).isSuccess(pool -> assertThat(pool.channels).isNull());
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(node));
    verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
        .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_indicate_when_keyspace_failed_on_all_channels() {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(3);

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node, new InvalidKeyspaceException("invalid keyspace"))
            .failure(node, new InvalidKeyspaceException("invalid keyspace"))
            .failure(node, new InvalidKeyspaceException("invalid keyspace"))
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);
    assertThatStage(poolFuture)
        .isSuccess(
            pool -> {
              assertThat(pool.isInvalidKeyspace()).isTrue();
              verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
                  .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);
            });
  }

  @Test
  public void should_fire_force_down_event_when_cluster_name_does_not_match() throws Exception {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(3);

    ClusterNameMismatchException error =
        new ClusterNameMismatchException(node.getEndPoint(), "actual", "expected");
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node, error)
            .failure(node, error)
            .failure(node, error)
            .build();

    ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);

    verify(eventBus, VERIFY_TIMEOUT)
        .fire(TopologyEvent.forceDown(node.getBroadcastRpcAddress().get()));
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(node));

    verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
        .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);
    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_when_init_incomplete() throws Exception {
    // Short delay so we don't have to wait in the test
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // Init: 1 channel fails, the other succeeds
            .failure(node, "mock channel init failure")
            .success(node, channel1)
            // 1st reconnection
            .pending(node, channel2Future)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // A reconnection should have been scheduled
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStarted(node));
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node));
    assertThat(pool.channels[0]).containsOnly(channel1);

    channel2Future.complete(channel2);
    factoryHelper.waitForCalls(node, 1);
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node));
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStopped(node));

    await().untilAsserted(() -> assertThat(pool.channels[0]).containsOnly(channel1, channel2));

    verify(nodeMetricUpdater, VERIFY_TIMEOUT)
        .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);
    factoryHelper.waitForCalls(node, 1);
  }
}
