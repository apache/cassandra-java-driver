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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

public class ChannelPoolKeyspaceTest extends ChannelPoolTestBase {

  @Test
  public void should_switch_keyspace_on_existing_channels() throws Exception {
    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(ADDRESS, channel1)
            .success(ADDRESS, channel2)
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    CqlIdentifier newKeyspace = CqlIdentifier.fromCql("new_keyspace");
    CompletionStage<Void> setKeyspaceFuture = pool.setKeyspace(newKeyspace);
    waitForPendingAdminTasks();

    Mockito.verify(channel1).setKeyspace(newKeyspace);
    Mockito.verify(channel2).setKeyspace(newKeyspace);

    assertThat(setKeyspaceFuture).isSuccess();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_switch_keyspace_on_pending_channels() throws Exception {
    Mockito.when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    Mockito.when(defaultProfile.getInt(CoreDriverOption.POOLING_LOCAL_CONNECTIONS)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    CompletableFuture<DriverChannel> channel1Future = new CompletableFuture<>();
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .failure(ADDRESS, "mock channel init failure")
            .failure(ADDRESS, "mock channel init failure")
            // reconnection
            .pending(ADDRESS, channel1Future)
            .pending(ADDRESS, channel2Future)
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(NODE, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(ADDRESS, 2);
    waitForPendingAdminTasks();

    assertThat(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // Check that reconnection has kicked in, but do not complete it yet
    Mockito.verify(reconnectionSchedule).nextDelay();
    Mockito.verify(eventBus).fire(ChannelEvent.reconnectionStarted(NODE));
    factoryHelper.waitForCalls(ADDRESS, 2);

    // Switch keyspace, it succeeds immediately since there is no active channel
    CqlIdentifier newKeyspace = CqlIdentifier.fromCql("new_keyspace");
    CompletionStage<Void> setKeyspaceFuture = pool.setKeyspace(newKeyspace);
    waitForPendingAdminTasks();
    assertThat(setKeyspaceFuture).isSuccess();

    // Now let the two channels succeed to complete the reconnection
    channel1Future.complete(channel1);
    channel2Future.complete(channel2);
    waitForPendingAdminTasks();

    Mockito.verify(eventBus).fire(ChannelEvent.reconnectionStopped(NODE));
    Mockito.verify(channel1).setKeyspace(newKeyspace);
    Mockito.verify(channel2).setKeyspace(newKeyspace);

    factoryHelper.verifyNoMoreCalls();
  }
}
