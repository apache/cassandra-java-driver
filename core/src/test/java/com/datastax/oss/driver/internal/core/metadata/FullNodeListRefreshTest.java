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

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FullNodeListRefreshTest {

  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);
  private static final InetSocketAddress ADDRESS3 = new InetSocketAddress("127.0.0.3", 9042);

  @Mock private InternalDriverContext context;
  @Mock protected MetricsFactory metricsFactory;

  private DefaultNode node1;
  private DefaultNode node2;
  private DefaultNode node3;

  @Before
  public void setup() {
    Mockito.when(context.getMetricsFactory()).thenReturn(metricsFactory);

    node1 = new DefaultNode(ADDRESS1, context);
    node2 = new DefaultNode(ADDRESS2, context);
    node3 = new DefaultNode(ADDRESS3, context);
  }

  @Test
  public void should_add_and_remove_nodes() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2), Collections.emptyMap(), null);
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder().withConnectAddress(ADDRESS2).build(),
            DefaultNodeInfo.builder().withConnectAddress(ADDRESS3).build());
    FullNodeListRefresh refresh = new FullNodeListRefresh(newInfos);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(ADDRESS2, ADDRESS3);
    assertThat(result.events)
        .containsOnly(NodeStateEvent.removed(node1), NodeStateEvent.added(node3));
  }

  @Test
  public void should_update_existing_nodes() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2), Collections.emptyMap(), null);

    UUID hostId1 = Uuids.random();
    UUID hostId2 = Uuids.random();
    UUID schemaVersion1 = Uuids.random();
    UUID schemaVersion2 = Uuids.random();
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder()
                .withConnectAddress(ADDRESS1)
                .withDatacenter("dc1")
                .withRack("rack1")
                .withHostId(hostId1)
                .withSchemaVersion(schemaVersion1)
                .build(),
            DefaultNodeInfo.builder()
                .withConnectAddress(ADDRESS2)
                .withDatacenter("dc1")
                .withRack("rack2")
                .withHostId(hostId2)
                .withSchemaVersion(schemaVersion2)
                .build());
    FullNodeListRefresh refresh = new FullNodeListRefresh(newInfos);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(ADDRESS1, ADDRESS2);
    assertThat(node1.getDatacenter()).isEqualTo("dc1");
    assertThat(node1.getRack()).isEqualTo("rack1");
    assertThat(node1.getHostId()).isEqualTo(hostId1);
    assertThat(node1.getSchemaVersion()).isEqualTo(schemaVersion1);
    assertThat(node2.getDatacenter()).isEqualTo("dc1");
    assertThat(node2.getRack()).isEqualTo("rack2");
    assertThat(node2.getHostId()).isEqualTo(hostId2);
    assertThat(node2.getSchemaVersion()).isEqualTo(schemaVersion2);
    assertThat(result.events).isEmpty();
  }
}
