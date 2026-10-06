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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.concurrent.CompletionStage;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ChannelPoolGracefulDisconnectTest extends ChannelPoolTestBase {

  private ChannelPool initPool(boolean gracefulDisconnectEnabled, DriverChannel... channels)
      throws Exception {
    when(defaultProfile.getBoolean(DefaultDriverOption.GRACEFUL_DISCONNECT_ENABLED, true))
        .thenReturn(gracefulDisconnectEnabled);
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
        .thenReturn(channels.length);

    MockChannelFactoryHelper.Builder factoryHelperBuilder =
        MockChannelFactoryHelper.builder(channelFactory);
    for (DriverChannel channel : channels) {
      factoryHelperBuilder.success(node, channel);
    }
    MockChannelFactoryHelper factoryHelper = factoryHelperBuilder.build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");
    factoryHelper.waitForCalls(node, channels.length);
    assertThatStage(poolFuture).isSuccess();
    return poolFuture.toCompletableFuture().get();
  }

  @Test
  public void should_request_graceful_disconnect_events_when_enabled() throws Exception {
    DriverChannel channel1 = newMockDriverChannel(1);
    initPool(true, channel1);

    ArgumentCaptor<DriverChannelOptions> optionsCaptor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    verify(channelFactory).connect(eq(node), optionsCaptor.capture());
    assertThat(optionsCaptor.getValue().eventTypes)
        .containsExactly(ProtocolConstants.EventType.GRACEFUL_DISCONNECT);
    assertThat(optionsCaptor.getValue().eventCallback).isNotNull();
  }

  @Test
  public void should_not_request_graceful_disconnect_events_when_disabled() throws Exception {
    DriverChannel channel1 = newMockDriverChannel(1);
    initPool(false, channel1);

    ArgumentCaptor<DriverChannelOptions> optionsCaptor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    verify(channelFactory).connect(eq(node), optionsCaptor.capture());
    assertThat(optionsCaptor.getValue().eventTypes).isEmpty();
  }

  @Test
  public void should_increment_metrics_when_event_received_on_query_connection() throws Exception {
    DriverChannel channel1 = newMockDriverChannel(1);
    initPool(true, channel1);

    ArgumentCaptor<DriverChannelOptions> optionsCaptor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    verify(channelFactory).connect(eq(node), optionsCaptor.capture());

    // Simulate the server sending GRACEFUL_DISCONNECT on the pooled connection. Per CEP-59 the
    // event stays local to the connection (the draining is handled by InFlightHandler, covered in
    // InFlightHandlerTest); the pool callback only records the metrics and does not touch the other
    // channels of the node.
    optionsCaptor
        .getValue()
        .eventCallback
        .onEvent(new com.datastax.oss.protocol.internal.response.event.GracefulDisconnectEvent());

    verify(nodeMetricUpdater, VERIFY_TIMEOUT)
        .incrementCounter(DefaultNodeMetric.GRACEFUL_DISCONNECTS, null);
    verify(sessionMetricUpdater, VERIFY_TIMEOUT)
        .incrementCounter(DefaultSessionMetric.GRACEFUL_DISCONNECTS, null);
  }
}
