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

import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RemoveNodeRefreshTest {

  @Mock private InternalDriverContext context;
  @Mock protected MetricsFactory metricsFactory;
  @Mock private ChannelFactory channelFactory;

  private DefaultNode node1;
  private DefaultNode node2;

  @Before
  public void setup() {
    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    when(context.getChannelFactory()).thenReturn(channelFactory);
    node1 = TestNodeFactory.newNode(1, context);
    node2 = TestNodeFactory.newNode(2, context);
  }

  @Test
  public void should_remove_existing_node() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1, node2.getHostId(), node2),
            Collections.emptyMap(),
            null,
            null);
    RemoveNodeRefresh refresh = new RemoveNodeRefresh(node2.getBroadcastRpcAddress().get());

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(node1.getHostId());
    assertThat(result.events).containsExactly(NodeStateEvent.removed(node2));
  }

  @Test
  public void should_not_remove_nonexistent_node() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1), Collections.emptyMap(), null, null);
    RemoveNodeRefresh refresh = new RemoveNodeRefresh(node2.getBroadcastRpcAddress().get());

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(node1.getHostId());
    assertThat(result.events).isEmpty();
  }
}
