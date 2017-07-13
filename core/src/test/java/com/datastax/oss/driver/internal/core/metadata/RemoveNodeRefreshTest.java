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

import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import org.junit.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class RemoveNodeRefreshTest {

  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);

  private static final DefaultNode node1 = new DefaultNode(ADDRESS1);
  private static final DefaultNode node2 = new DefaultNode(ADDRESS2);

  @Test
  public void should_remove_existing_node() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2));
    RemoveNodeRefresh refresh = new RemoveNodeRefresh(oldMetadata, ADDRESS2, "test");

    // When
    refresh.compute();

    // Then
    assertThat(refresh.newMetadata.getNodes()).containsOnlyKeys(ADDRESS1);
    assertThat(refresh.events).containsExactly(NodeStateEvent.removed(node2));
  }

  @Test
  public void should_not_remove_nonexistent_node() {
    // Given
    DefaultMetadata oldMetadata = new DefaultMetadata(ImmutableMap.of(ADDRESS1, node1));
    RemoveNodeRefresh refresh = new RemoveNodeRefresh(oldMetadata, ADDRESS2, "test");

    // When
    refresh.compute();

    // Then
    assertThat(refresh.newMetadata.getNodes()).containsOnlyKeys(ADDRESS1);
    assertThat(refresh.events).isEmpty();
  }
}
