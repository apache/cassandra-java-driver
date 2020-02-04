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
package com.datastax.oss.driver.internal.core.control;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.channel.EventCallback;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.event.SchemaChangeEvent;
import com.datastax.oss.protocol.internal.response.event.StatusChangeEvent;
import com.datastax.oss.protocol.internal.response.event.TopologyChangeEvent;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ControlConnectionEventsTest extends ControlConnectionTestBase {

  @Test
  public void should_register_for_all_events_if_topology_requested() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    ArgumentCaptor<DriverChannelOptions> optionsCaptor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    when(channelFactory.connect(eq(node1), optionsCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(channel1));

    // When
    controlConnection.init(true, false, false);

    // Then
    await()
        .untilAsserted(
            () -> {
              DriverChannelOptions channelOptions = optionsCaptor.getValue();
              assertThat(channelOptions.eventTypes)
                  .containsExactly(
                      ProtocolConstants.EventType.SCHEMA_CHANGE,
                      ProtocolConstants.EventType.STATUS_CHANGE,
                      ProtocolConstants.EventType.TOPOLOGY_CHANGE);
              assertThat(channelOptions.eventCallback).isEqualTo(controlConnection);
            });
  }

  @Test
  public void should_register_for_schema_events_only_if_topology_not_requested() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    ArgumentCaptor<DriverChannelOptions> optionsCaptor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    when(channelFactory.connect(eq(node1), optionsCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(channel1));

    // When
    controlConnection.init(false, false, false);

    // Then
    await()
        .untilAsserted(
            () -> {
              DriverChannelOptions channelOptions = optionsCaptor.getValue();
              assertThat(channelOptions.eventTypes)
                  .containsExactly(ProtocolConstants.EventType.SCHEMA_CHANGE);
              assertThat(channelOptions.eventCallback).isEqualTo(controlConnection);
            });
  }

  @Test
  public void should_process_status_change_events() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    ArgumentCaptor<DriverChannelOptions> optionsCaptor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    when(channelFactory.connect(eq(node1), optionsCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(channel1));
    controlConnection.init(true, false, false);
    await().until(() -> optionsCaptor.getValue() != null);
    EventCallback callback = optionsCaptor.getValue().eventCallback;
    StatusChangeEvent event =
        new StatusChangeEvent(ProtocolConstants.StatusChangeType.UP, ADDRESS1);

    // When
    callback.onEvent(event);

    // Then
    verify(eventBus).fire(TopologyEvent.suggestUp(ADDRESS1));
  }

  @Test
  public void should_process_topology_change_events() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    ArgumentCaptor<DriverChannelOptions> optionsCaptor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    when(channelFactory.connect(eq(node1), optionsCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(channel1));
    controlConnection.init(true, false, false);
    await().until(() -> optionsCaptor.getValue() != null);
    EventCallback callback = optionsCaptor.getValue().eventCallback;
    TopologyChangeEvent event =
        new TopologyChangeEvent(ProtocolConstants.TopologyChangeType.NEW_NODE, ADDRESS1);

    // When
    callback.onEvent(event);

    // Then
    verify(eventBus).fire(TopologyEvent.suggestAdded(ADDRESS1));
  }

  @Test
  public void should_process_schema_change_events() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    ArgumentCaptor<DriverChannelOptions> optionsCaptor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    when(channelFactory.connect(eq(node1), optionsCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(channel1));
    controlConnection.init(false, false, false);
    await().until(() -> optionsCaptor.getValue() != null);
    EventCallback callback = optionsCaptor.getValue().eventCallback;
    SchemaChangeEvent event =
        new SchemaChangeEvent(
            ProtocolConstants.SchemaChangeType.CREATED,
            ProtocolConstants.SchemaChangeTarget.FUNCTION,
            "ks",
            "fn",
            ImmutableList.of("text", "text"));

    // When
    callback.onEvent(event);

    // Then
    verify(metadataManager).refreshSchema("ks", false, false);
  }
}
