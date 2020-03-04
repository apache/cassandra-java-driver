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
package com.datastax.oss.driver.internal.core.metadata;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AddNodeRefreshTest {

  @Mock private InternalDriverContext context;
  @Mock protected MetricsFactory metricsFactory;
  @Mock private ChannelFactory channelFactory;

  private DefaultNode node1;

  @Before
  public void setup() {
    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    when(context.getChannelFactory()).thenReturn(channelFactory);
    node1 = TestNodeFactory.newNode(1, context);
  }

  @Test
  public void should_add_new_node() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1), Collections.emptyMap(), null, null);
    UUID newHostId = Uuids.random();
    DefaultEndPoint newEndPoint = TestNodeFactory.newEndPoint(2);
    UUID newSchemaVersion = Uuids.random();
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withHostId(newHostId)
            .withEndPoint(newEndPoint)
            .withDatacenter("dc1")
            .withRack("rack2")
            .withSchemaVersion(newSchemaVersion)
            .build();
    AddNodeRefresh refresh = new AddNodeRefresh(newNodeInfo);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(node1.getHostId(), newHostId);
    Node node2 = newNodes.get(newHostId);
    assertThat(node2.getEndPoint()).isEqualTo(newEndPoint);
    assertThat(node2.getDatacenter()).isEqualTo("dc1");
    assertThat(node2.getRack()).isEqualTo("rack2");
    assertThat(node2.getHostId()).isEqualTo(newHostId);
    assertThat(node2.getSchemaVersion()).isEqualTo(newSchemaVersion);
    assertThat(result.events).containsExactly(NodeStateEvent.added((DefaultNode) node2));
  }

  @Test
  public void should_not_add_existing_node_with_same_id_and_endpoint() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1), Collections.emptyMap(), null, null);
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withHostId(node1.getHostId())
            .withEndPoint(node1.getEndPoint())
            .withDatacenter("dc1")
            .withRack("rack2")
            .build();
    AddNodeRefresh refresh = new AddNodeRefresh(newNodeInfo);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(node1.getHostId());
    // Info is not copied over:
    assertThat(node1.getDatacenter()).isNull();
    assertThat(node1.getRack()).isNull();
    assertThat(result.events).isEmpty();
  }

  @Test
  public void should_add_existing_node_with_same_id_but_different_endpoint() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1), Collections.emptyMap(), null, null);
    DefaultEndPoint newEndPoint = TestNodeFactory.newEndPoint(2);
    InetSocketAddress newBroadcastRpcAddress = newEndPoint.resolve();
    UUID newSchemaVersion = Uuids.random();
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withHostId(node1.getHostId())
            .withEndPoint(newEndPoint)
            .withDatacenter("dc1")
            .withRack("rack2")
            .withSchemaVersion(newSchemaVersion)
            .withBroadcastRpcAddress(newBroadcastRpcAddress)
            .build();
    AddNodeRefresh refresh = new AddNodeRefresh(newNodeInfo);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).hasSize(1).containsEntry(node1.getHostId(), node1);
    assertThat(node1.getEndPoint()).isEqualTo(newEndPoint);
    assertThat(node1.getDatacenter()).isEqualTo("dc1");
    assertThat(node1.getRack()).isEqualTo("rack2");
    assertThat(node1.getSchemaVersion()).isEqualTo(newSchemaVersion);
    assertThat(result.events).containsExactly(TopologyEvent.suggestUp(newBroadcastRpcAddress));
  }
}
