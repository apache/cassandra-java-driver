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

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

public class ChannelPoolKeyspaceTest extends ChannelPoolTestBase {

  @Test
  public void should_switch_keyspace_on_existing_channels() throws Exception {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node, channel1)
            .success(node, channel2)
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 2);

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();
    assertThat(pool.channels).containsOnly(channel1, channel2);

    CqlIdentifier newKeyspace = CqlIdentifier.fromCql("new_keyspace");
    CompletionStage<Void> setKeyspaceFuture = pool.setKeyspace(newKeyspace);

    verify(channel1, VERIFY_TIMEOUT).setKeyspace(newKeyspace);
    verify(channel2, VERIFY_TIMEOUT).setKeyspace(newKeyspace);

    assertThatStage(setKeyspaceFuture).isSuccess();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_switch_keyspace_on_pending_channels() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    CompletableFuture<DriverChannel> channel1Future = new CompletableFuture<>();
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .failure(node, "mock channel init failure")
            .failure(node, "mock channel init failure")
            // reconnection
            .pending(node, channel1Future)
            .pending(node, channel2Future)
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 2);

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // Check that reconnection has kicked in, but do not complete it yet
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStarted(node));
    factoryHelper.waitForCalls(node, 2);

    // Switch keyspace, it succeeds immediately since there is no active channel
    CqlIdentifier newKeyspace = CqlIdentifier.fromCql("new_keyspace");
    CompletionStage<Void> setKeyspaceFuture = pool.setKeyspace(newKeyspace);
    assertThatStage(setKeyspaceFuture).isSuccess();

    // Now let the two channels succeed to complete the reconnection
    channel1Future.complete(channel1);
    channel2Future.complete(channel2);

    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStopped(node));
    verify(channel1, VERIFY_TIMEOUT).setKeyspace(newKeyspace);
    verify(channel2, VERIFY_TIMEOUT).setKeyspace(newKeyspace);

    factoryHelper.verifyNoMoreCalls();
  }
}
