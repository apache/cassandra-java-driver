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

import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RemoveNodeRefreshTest {

  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);

  @Mock private InternalDriverContext context;
  @Mock protected MetricsFactory metricsFactory;

  private DefaultNode node1;
  private DefaultNode node2;

  @Before
  public void setup() {
    Mockito.when(context.getMetricsFactory()).thenReturn(metricsFactory);
    node1 = new DefaultNode(ADDRESS1, context);
    node2 = new DefaultNode(ADDRESS2, context);
  }

  @Test
  public void should_remove_existing_node() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2), Collections.emptyMap(), null);
    RemoveNodeRefresh refresh = new RemoveNodeRefresh(ADDRESS2);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(ADDRESS1);
    assertThat(result.events).containsExactly(NodeStateEvent.removed(node2));
  }

  @Test
  public void should_not_remove_nonexistent_node() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, node1), Collections.emptyMap(), null);
    RemoveNodeRefresh refresh = new RemoveNodeRefresh(ADDRESS2);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(ADDRESS1);
    assertThat(result.events).isEmpty();
  }
}
