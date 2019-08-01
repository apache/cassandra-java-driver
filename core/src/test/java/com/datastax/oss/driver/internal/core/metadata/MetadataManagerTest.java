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
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.SchemaParserFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaQueriesFactory;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MetadataManagerTest {

  // Don't use 1 because that's the default when no contact points are provided
  private static final EndPoint END_POINT2 = TestNodeFactory.newEndPoint(2);
  private static final EndPoint END_POINT3 = TestNodeFactory.newEndPoint(3);

  @Mock private InternalDriverContext context;
  @Mock private NettyOptions nettyOptions;
  @Mock private TopologyMonitor topologyMonitor;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultProfile;
  @Mock private EventBus eventBus;
  @Mock private SchemaQueriesFactory schemaQueriesFactory;
  @Mock private SchemaParserFactory schemaParserFactory;
  @Mock protected MetricsFactory metricsFactory;

  private DefaultEventLoopGroup adminEventLoopGroup;

  private TestMetadataManager metadataManager;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    adminEventLoopGroup = new DefaultEventLoopGroup(1);
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    when(context.getNettyOptions()).thenReturn(nettyOptions);

    when(context.getTopologyMonitor()).thenReturn(topologyMonitor);

    when(defaultProfile.getDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW))
        .thenReturn(Duration.ZERO);
    when(defaultProfile.getInt(DefaultDriverOption.METADATA_SCHEMA_MAX_EVENTS)).thenReturn(1);
    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(context.getConfig()).thenReturn(config);

    when(context.getEventBus()).thenReturn(eventBus);
    when(context.getSchemaQueriesFactory()).thenReturn(schemaQueriesFactory);
    when(context.getSchemaParserFactory()).thenReturn(schemaParserFactory);

    when(context.getMetricsFactory()).thenReturn(metricsFactory);

    metadataManager = new TestMetadataManager(context);
  }

  @After
  public void teardown() {
    adminEventLoopGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
  }

  @Test
  public void should_add_contact_points() {
    // When
    metadataManager.addContactPoints(ImmutableSet.of(END_POINT2));

    // Then
    assertThat(metadataManager.getContactPoints())
        .extracting(Node::getEndPoint)
        .containsOnly(END_POINT2);
    assertThat(metadataManager.wasImplicitContactPoint()).isFalse();
  }

  @Test
  public void should_use_default_if_no_contact_points_provided() {
    // When
    metadataManager.addContactPoints(Collections.emptySet());

    // Then
    assertThat(metadataManager.getContactPoints())
        .extracting(Node::getEndPoint)
        .containsOnly(MetadataManager.DEFAULT_CONTACT_POINT);
    assertThat(metadataManager.wasImplicitContactPoint()).isTrue();
  }

  @Test
  public void should_copy_contact_points_on_refresh_of_all_nodes() {
    // Given
    // Run previous scenario to trigger the addition of the default contact point:
    should_use_default_if_no_contact_points_provided();

    NodeInfo info1 = mock(NodeInfo.class);
    NodeInfo info2 = mock(NodeInfo.class);
    List<NodeInfo> infos = ImmutableList.of(info1, info2);
    when(topologyMonitor.refreshNodeList()).thenReturn(CompletableFuture.completedFuture(infos));

    // When
    CompletionStage<Void> refreshNodesFuture = metadataManager.refreshNodes();
    waitForPendingAdminTasks();

    // Then
    assertThatStage(refreshNodesFuture).isSuccess();
    assertThat(metadataManager.refreshes).hasSize(1);
    InitialNodeListRefresh refresh = (InitialNodeListRefresh) metadataManager.refreshes.get(0);
    assertThat(refresh.contactPoints)
        .extracting(Node::getEndPoint)
        .containsOnly(MetadataManager.DEFAULT_CONTACT_POINT);
    assertThat(refresh.nodeInfos).containsExactlyInAnyOrder(info1, info2);
  }

  @Test
  public void should_refresh_all_nodes() {
    // Given
    // Run previous scenario to trigger the addition of the default contact point and a first
    // refresh:
    should_copy_contact_points_on_refresh_of_all_nodes();
    // Discard that first refresh, we don't really care about it in the context of this test, only
    // that the next one won't be the first
    metadataManager.refreshes.clear();

    NodeInfo info1 = mock(NodeInfo.class);
    NodeInfo info2 = mock(NodeInfo.class);
    List<NodeInfo> infos = ImmutableList.of(info1, info2);
    when(topologyMonitor.refreshNodeList()).thenReturn(CompletableFuture.completedFuture(infos));

    // When
    CompletionStage<Void> refreshNodesFuture = metadataManager.refreshNodes();
    waitForPendingAdminTasks();

    // Then
    assertThatStage(refreshNodesFuture).isSuccess();
    assertThat(metadataManager.refreshes).hasSize(1);
    FullNodeListRefresh refresh = (FullNodeListRefresh) metadataManager.refreshes.get(0);
    assertThat(refresh.nodeInfos).containsExactlyInAnyOrder(info1, info2);
  }

  @Test
  public void should_refresh_single_node() {
    // Given
    Node node = TestNodeFactory.newNode(2, context);
    NodeInfo info = mock(NodeInfo.class);
    when(info.getDatacenter()).thenReturn("dc1");
    when(info.getHostId()).thenReturn(UUID.randomUUID());
    when(info.getEndPoint()).thenReturn(node.getEndPoint());
    when(topologyMonitor.refreshNode(node))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(info)));

    // When
    CompletionStage<Void> refreshNodeFuture = metadataManager.refreshNode(node);

    // Then
    // the info should have been copied to the node
    assertThatStage(refreshNodeFuture).isSuccess();
    verify(info, timeout(500)).getDatacenter();
    assertThat(node.getDatacenter()).isEqualTo("dc1");
  }

  @Test
  public void should_ignore_node_refresh_if_topology_monitor_does_not_have_info() {
    // Given
    Node node = mock(Node.class);
    when(topologyMonitor.refreshNode(node))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    // When
    CompletionStage<Void> refreshNodeFuture = metadataManager.refreshNode(node);

    // Then
    assertThatStage(refreshNodeFuture).isSuccess();
  }

  @Test
  public void should_add_node() {
    // Given
    InetSocketAddress broadcastRpcAddress = ((InetSocketAddress) END_POINT2.resolve());
    NodeInfo info = mock(NodeInfo.class);
    when(info.getBroadcastRpcAddress()).thenReturn(Optional.of(broadcastRpcAddress));
    when(topologyMonitor.getNewNodeInfo(broadcastRpcAddress))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(info)));

    // When
    metadataManager.addNode(broadcastRpcAddress);
    waitForPendingAdminTasks();

    // Then
    assertThat(metadataManager.refreshes).hasSize(1);
    AddNodeRefresh refresh = (AddNodeRefresh) metadataManager.refreshes.get(0);
    assertThat(refresh.newNodeInfo).isEqualTo(info);
  }

  @Test
  public void should_not_add_node_if_broadcast_rpc_address_does_not_match() {
    // Given
    InetSocketAddress broadcastRpcAddress2 = ((InetSocketAddress) END_POINT2.resolve());
    InetSocketAddress broadcastRpcAddress3 = ((InetSocketAddress) END_POINT3.resolve());
    NodeInfo info = mock(NodeInfo.class);
    when(topologyMonitor.getNewNodeInfo(broadcastRpcAddress2))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(info)));
    when(info.getBroadcastRpcAddress())
        .thenReturn(
            Optional.of(broadcastRpcAddress3) // Does not match the address we got the info with
            );

    // When
    metadataManager.addNode(broadcastRpcAddress2);
    waitForPendingAdminTasks();

    // Then
    assertThat(metadataManager.refreshes).isEmpty();
  }

  @Test
  public void should_not_add_node_if_topology_monitor_does_not_have_info() {
    // Given
    InetSocketAddress broadcastRpcAddress2 = ((InetSocketAddress) END_POINT2.resolve());
    when(topologyMonitor.getNewNodeInfo(broadcastRpcAddress2))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    // When
    metadataManager.addNode(broadcastRpcAddress2);
    waitForPendingAdminTasks();

    // Then
    assertThat(metadataManager.refreshes).isEmpty();
  }

  @Test
  public void should_remove_node() {
    // Given
    InetSocketAddress broadcastRpcAddress2 = ((InetSocketAddress) END_POINT2.resolve());

    // When
    metadataManager.removeNode(broadcastRpcAddress2);
    waitForPendingAdminTasks();

    // Then
    assertThat(metadataManager.refreshes).hasSize(1);
    RemoveNodeRefresh refresh = (RemoveNodeRefresh) metadataManager.refreshes.get(0);
    assertThat(refresh.broadcastRpcAddressToRemove).isEqualTo(broadcastRpcAddress2);
  }

  private static class TestMetadataManager extends MetadataManager {

    private List<MetadataRefresh> refreshes = new CopyOnWriteArrayList<>();

    public TestMetadataManager(InternalDriverContext context) {
      super(context);
    }

    @Override
    Void apply(MetadataRefresh refresh) {
      // Do not execute refreshes, just store them for inspection in the test
      refreshes.add(refresh);
      return null;
    }
  }

  // Wait for all the tasks on the pool's admin executor to complete.
  private void waitForPendingAdminTasks() {
    // This works because the event loop group is single-threaded
    Future<?> f = adminEventLoopGroup.schedule(() -> null, 5, TimeUnit.NANOSECONDS);
    try {
      Uninterruptibles.getUninterruptibly(f, 100, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      fail("unexpected error", e.getCause());
    } catch (TimeoutException e) {
      fail("timed out while waiting for admin tasks to complete", e);
    }
  }
}
