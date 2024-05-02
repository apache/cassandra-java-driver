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
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InitialNodeListRefreshTest {

  @Mock private InternalDriverContext context;
  @Mock protected MetricsFactory metricsFactory;
  @Mock private ChannelFactory channelFactory;

  private DefaultNode contactPoint1;
  private DefaultNode contactPoint2;
  private EndPoint endPoint3;
  private UUID hostId1;
  private UUID hostId2;
  private UUID hostId3;

  @Before
  public void setup() {
    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    when(context.getChannelFactory()).thenReturn(channelFactory);

    contactPoint1 = TestNodeFactory.newContactPoint(1, context);
    contactPoint2 = TestNodeFactory.newContactPoint(2, context);

    endPoint3 = TestNodeFactory.newEndPoint(3);
    hostId1 = UUID.randomUUID();
    hostId2 = UUID.randomUUID();
    hostId3 = UUID.randomUUID();
  }

  @Test
  public void should_copy_contact_points() {
    // Given
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder()
                .withEndPoint(contactPoint1.getEndPoint())
                // in practice there are more fields, but hostId is enough to validate the logic
                .withHostId(hostId1)
                .build(),
            DefaultNodeInfo.builder()
                .withEndPoint(contactPoint2.getEndPoint())
                .withHostId(hostId2)
                .build());
    InitialNodeListRefresh refresh =
        new InitialNodeListRefresh(newInfos, ImmutableSet.of(contactPoint1, contactPoint2));

    // When
    MetadataRefresh.Result result = refresh.compute(DefaultMetadata.EMPTY, false, context);

    // Then
    // contact points have been copied to the metadata, and completed with missing information
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(hostId1, hostId2);
    assertThat(newNodes.get(hostId1)).isEqualTo(contactPoint1);
    assertThat(contactPoint1.getHostId()).isEqualTo(hostId1);
    assertThat(newNodes.get(hostId2)).isEqualTo(contactPoint2);
    assertThat(contactPoint2.getHostId()).isEqualTo(hostId2);
  }

  @Test
  public void should_add_other_nodes() {
    // Given
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder()
                .withEndPoint(contactPoint1.getEndPoint())
                // in practice there are more fields, but hostId is enough to validate the logic
                .withHostId(hostId1)
                .build(),
            DefaultNodeInfo.builder()
                .withEndPoint(contactPoint2.getEndPoint())
                .withHostId(hostId2)
                .build(),
            DefaultNodeInfo.builder().withEndPoint(endPoint3).withHostId(hostId3).build());
    InitialNodeListRefresh refresh =
        new InitialNodeListRefresh(newInfos, ImmutableSet.of(contactPoint1, contactPoint2));

    // When
    MetadataRefresh.Result result = refresh.compute(DefaultMetadata.EMPTY, false, context);

    // Then
    // new node created in addition to the contact points
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(hostId1, hostId2, hostId3);
    Node node3 = newNodes.get(hostId3);
    assertThat(node3.getEndPoint()).isEqualTo(endPoint3);
    assertThat(node3.getHostId()).isEqualTo(hostId3);
  }

  @Test
  public void should_ignore_duplicate_host_ids() {
    // Given
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder()
                .withEndPoint(contactPoint1.getEndPoint())
                // in practice there are more fields, but hostId is enough to validate the logic
                .withHostId(hostId1)
                .withDatacenter("dc1")
                .build(),
            DefaultNodeInfo.builder()
                .withEndPoint(contactPoint1.getEndPoint())
                .withDatacenter("dc2")
                .withHostId(hostId1)
                .build());
    InitialNodeListRefresh refresh =
        new InitialNodeListRefresh(newInfos, ImmutableSet.of(contactPoint1));

    // When
    MetadataRefresh.Result result = refresh.compute(DefaultMetadata.EMPTY, false, context);

    // Then
    // only the first nodeInfo should have been copied
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(hostId1);
    assertThat(newNodes.get(hostId1)).isEqualTo(contactPoint1);
    assertThat(contactPoint1.getHostId()).isEqualTo(hostId1);
    assertThat(contactPoint1.getDatacenter()).isEqualTo("dc1");
  }
}
