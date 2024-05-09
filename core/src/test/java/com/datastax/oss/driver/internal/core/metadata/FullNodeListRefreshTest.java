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
package com.datastax.oss.driver.internal.core.metadata;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FullNodeListRefreshTest {

  @Mock private InternalDriverContext context;
  @Mock protected MetricsFactory metricsFactory;
  @Mock private ChannelFactory channelFactory;

  private DefaultNode node1;
  private DefaultNode node2;
  private EndPoint endPoint3;
  private UUID hostId3;

  @Before
  public void setup() {
    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    when(context.getChannelFactory()).thenReturn(channelFactory);

    node1 = TestNodeFactory.newNode(1, context);
    node2 = TestNodeFactory.newNode(2, context);

    endPoint3 = TestNodeFactory.newEndPoint(3);
    hostId3 = UUID.randomUUID();
  }

  @Test
  public void should_add_and_remove_nodes() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1, node2.getHostId(), node2),
            Collections.emptyMap(),
            null,
            null);
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder()
                .withEndPoint(node2.getEndPoint())
                .withHostId(node2.getHostId())
                .build(),
            DefaultNodeInfo.builder().withEndPoint(endPoint3).withHostId(hostId3).build());
    FullNodeListRefresh refresh = new FullNodeListRefresh(newInfos);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(node2.getHostId(), hostId3);
    DefaultNode node3 = (DefaultNode) result.newMetadata.getNodes().get(hostId3);
    assertThat(result.events)
        .containsOnly(NodeStateEvent.removed(node1), NodeStateEvent.added(node3));
  }

  @Test
  public void should_update_existing_nodes() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1, node2.getHostId(), node2),
            Collections.emptyMap(),
            null,
            null);

    UUID schemaVersion1 = Uuids.random();
    UUID schemaVersion2 = Uuids.random();
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder()
                .withEndPoint(node1.getEndPoint())
                .withDatacenter("dc1")
                .withRack("rack1")
                .withHostId(node1.getHostId())
                .withSchemaVersion(schemaVersion1)
                .build(),
            DefaultNodeInfo.builder()
                .withEndPoint(node2.getEndPoint())
                .withDatacenter("dc1")
                .withRack("rack2")
                .withHostId(node2.getHostId())
                .withSchemaVersion(schemaVersion2)
                .build());
    FullNodeListRefresh refresh = new FullNodeListRefresh(newInfos);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes())
        .containsOnlyKeys(node1.getHostId(), node2.getHostId());
    assertThat(node1.getDatacenter()).isEqualTo("dc1");
    assertThat(node1.getRack()).isEqualTo("rack1");
    assertThat(node1.getSchemaVersion()).isEqualTo(schemaVersion1);
    assertThat(node2.getDatacenter()).isEqualTo("dc1");
    assertThat(node2.getRack()).isEqualTo("rack2");
    assertThat(node2.getSchemaVersion()).isEqualTo(schemaVersion2);
    assertThat(result.events).isEmpty();
  }

  @Test
  public void should_ignore_duplicate_host_ids() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1, node2.getHostId(), node2),
            Collections.emptyMap(),
            null,
            null);

    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder()
                .withEndPoint(node1.getEndPoint())
                .withDatacenter("dc1")
                .withRack("rack1")
                .withHostId(node1.getHostId())
                .build(),
            DefaultNodeInfo.builder()
                .withEndPoint(node2.getEndPoint())
                .withDatacenter("dc1")
                .withRack("rack2")
                .withHostId(node2.getHostId())
                .build(),
            // Duplicate host id for node 2, should be ignored:
            DefaultNodeInfo.builder()
                .withEndPoint(node2.getEndPoint())
                .withDatacenter("dc1")
                .withRack("rack3")
                .withHostId(node2.getHostId())
                .build());
    FullNodeListRefresh refresh = new FullNodeListRefresh(newInfos);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes())
        .containsOnlyKeys(node1.getHostId(), node2.getHostId());
    assertThat(node1.getDatacenter()).isEqualTo("dc1");
    assertThat(node1.getRack()).isEqualTo("rack1");
    assertThat(node2.getDatacenter()).isEqualTo("dc1");
    assertThat(node2.getRack()).isEqualTo("rack2");
    assertThat(result.events).isEmpty();
  }
}
