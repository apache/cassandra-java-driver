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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Map;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class AddNodeRefreshTest {
  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);

  private static final DefaultNode node1 = new DefaultNode(ADDRESS1);

  @Test
  public void should_add_new_node() {
    // Given
    DefaultMetadata oldMetadata = new DefaultMetadata(ImmutableMap.of(ADDRESS1, node1));
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withConnectAddress(ADDRESS2)
            .withDatacenter("dc1")
            .withRack("rack2")
            .build();
    AddNodeRefresh refresh = new AddNodeRefresh(oldMetadata, newNodeInfo, "test");

    // When
    refresh.compute();

    // Then
    Map<InetSocketAddress, Node> newNodes = refresh.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(ADDRESS1, ADDRESS2);
    Node node2 = newNodes.get(ADDRESS2);
    assertThat(node2.getDatacenter()).isEqualTo("dc1");
    assertThat(node2.getRack()).isEqualTo("rack2");
    assertThat(refresh.events).containsExactly(NodeStateEvent.added((DefaultNode) node2));
  }

  @Test
  public void should_not_add_existing_node() {
    // Given
    DefaultMetadata oldMetadata = new DefaultMetadata(ImmutableMap.of(ADDRESS1, node1));
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withConnectAddress(ADDRESS1)
            .withDatacenter("dc1")
            .withRack("rack2")
            .build();
    AddNodeRefresh refresh = new AddNodeRefresh(oldMetadata, newNodeInfo, "test");

    // When
    refresh.compute();

    // Then
    assertThat(refresh.newMetadata.getNodes()).containsOnlyKeys(ADDRESS1);
    // Info is not copied over:
    assertThat(node1.getDatacenter()).isNull();
    assertThat(node1.getRack()).isNull();
    assertThat(refresh.events).isEmpty();
  }
}
