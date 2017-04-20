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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class FullNodeListRefreshTest {

  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);
  private static final InetSocketAddress ADDRESS3 = new InetSocketAddress("127.0.0.3", 9042);

  private static final DefaultNode node1 = new DefaultNode(ADDRESS1);
  private static final DefaultNode node2 = new DefaultNode(ADDRESS2);
  private static final DefaultNode node3 = new DefaultNode(ADDRESS3);

  @Test
  public void should_add_and_remove_nodes() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2));
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder().withConnectAddress(ADDRESS2).build(),
            DefaultNodeInfo.builder().withConnectAddress(ADDRESS3).build());
    FullNodeListRefresh refresh = new FullNodeListRefresh(oldMetadata, newInfos);

    // When
    refresh.compute();

    // Then
    assertThat(refresh.newMetadata.getNodes()).containsOnlyKeys(ADDRESS2, ADDRESS3);
    assertThat(refresh.events)
        .containsOnly(NodeStateEvent.removed(node1), NodeStateEvent.added(node3));
  }

  @Test
  public void should_update_existing_nodes() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2));
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder()
                .withConnectAddress(ADDRESS1)
                .withDatacenter("dc1")
                .withRack("rack1")
                .build(),
            DefaultNodeInfo.builder()
                .withConnectAddress(ADDRESS2)
                .withDatacenter("dc1")
                .withRack("rack2")
                .build());
    FullNodeListRefresh refresh = new FullNodeListRefresh(oldMetadata, newInfos);

    // When
    refresh.compute();

    // Then
    assertThat(refresh.newMetadata.getNodes()).containsOnlyKeys(ADDRESS1, ADDRESS2);
    assertThat(node1.getDatacenter()).isEqualTo("dc1");
    assertThat(node1.getRack()).isEqualTo("rack1");
    assertThat(node2.getDatacenter()).isEqualTo("dc1");
    assertThat(node2.getRack()).isEqualTo("rack2");
    assertThat(refresh.events).isEmpty();
  }
}
