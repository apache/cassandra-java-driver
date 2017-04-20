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
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.timeout;

public class MetadataManagerTest {

  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);

  @Mock private InternalDriverContext context;
  @Mock private NettyOptions nettyOptions;
  @Mock private TopologyMonitor topologyMonitor;

  private DefaultEventLoopGroup adminEventLoopGroup;

  private TestMetadataManager metadataManager;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);

    adminEventLoopGroup = new DefaultEventLoopGroup(1);
    Mockito.when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    Mockito.when(context.nettyOptions()).thenReturn(nettyOptions);

    Mockito.when(context.topologyMonitor()).thenReturn(topologyMonitor);

    metadataManager = new TestMetadataManager(context);
  }

  @Test
  public void should_add_contact_points() {
    // When
    CompletionStage<Void> addContactPointsFuture =
        metadataManager.addContactPoints(ImmutableSet.of(ADDRESS1));
    waitForPendingAdminTasks();

    // Then
    assertThat(addContactPointsFuture).isSuccess();
    assertThat(metadataManager.refreshes).hasSize(1);
    InitContactPointsRefresh refresh =
        ((InitContactPointsRefresh) metadataManager.refreshes.get(0));
    assertThat(refresh.contactPoints).containsExactlyInAnyOrder(ADDRESS1);
  }

  @Test
  public void should_refresh_all_nodes() {
    // Given
    NodeInfo info1 = Mockito.mock(NodeInfo.class);
    NodeInfo info2 = Mockito.mock(NodeInfo.class);
    List<NodeInfo> infos = ImmutableList.of(info1, info2);
    Mockito.when(topologyMonitor.refreshNodeList())
        .thenReturn(CompletableFuture.completedFuture(infos));

    // When
    CompletionStage<Void> refreshNodesFuture = metadataManager.refreshNodes();
    waitForPendingAdminTasks();

    // Then
    assertThat(refreshNodesFuture).isSuccess();
    assertThat(metadataManager.refreshes).hasSize(1);
    FullNodeListRefresh refresh = (FullNodeListRefresh) metadataManager.refreshes.get(0);
    assertThat(refresh.nodeInfos).containsExactlyInAnyOrder(info1, info2);
  }

  @Test
  public void should_refresh_single_node() {
    // Given
    Node node = new DefaultNode(ADDRESS1);
    NodeInfo info = Mockito.mock(NodeInfo.class);
    Mockito.when(info.getDatacenter()).thenReturn("dc1");
    Mockito.when(topologyMonitor.refreshNode(node))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(info)));

    // When
    CompletionStage<Void> refreshNodeFuture = metadataManager.refreshNode(node);

    // Then
    // the info should have been copied to the node
    assertThat(refreshNodeFuture).isSuccess();
    Mockito.verify(info, timeout(100)).getDatacenter();
    assertThat(node.getDatacenter()).isEqualTo("dc1");
  }

  @Test
  public void should_ignore_node_refresh_if_topology_monitor_does_not_have_info() {
    // Given
    Node node = Mockito.mock(Node.class);
    Mockito.when(topologyMonitor.refreshNode(node))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    // When
    CompletionStage<Void> refreshNodeFuture = metadataManager.refreshNode(node);

    // Then
    assertThat(refreshNodeFuture).isSuccess();
  }

  @Test
  public void should_add_node() {
    // Given
    NodeInfo info = Mockito.mock(NodeInfo.class);
    Mockito.when(info.getConnectAddress()).thenReturn(ADDRESS1);
    Mockito.when(topologyMonitor.getNewNodeInfo(ADDRESS1))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(info)));

    // When
    metadataManager.addNode(ADDRESS1);
    waitForPendingAdminTasks();

    // Then
    assertThat(metadataManager.refreshes).hasSize(1);
    AddNodeRefresh refresh = (AddNodeRefresh) metadataManager.refreshes.get(0);
    assertThat(refresh.newNodeInfo).isEqualTo(info);
  }

  @Test
  public void should_not_add_node_if_connect_address_does_not_match() {
    // Given
    NodeInfo info = Mockito.mock(NodeInfo.class);
    Mockito.when(topologyMonitor.getNewNodeInfo(ADDRESS1))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(info)));
    Mockito.when(info.getConnectAddress())
        .thenReturn(
            ADDRESS2 // Does not match the address we got the info with
            );

    // When
    metadataManager.addNode(ADDRESS1);
    waitForPendingAdminTasks();

    // Then
    assertThat(metadataManager.refreshes).isEmpty();
  }

  @Test
  public void should_not_add_node_if_topology_monitor_does_not_have_info() {
    // Given
    Mockito.when(topologyMonitor.getNewNodeInfo(ADDRESS1))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    // When
    metadataManager.addNode(ADDRESS1);
    waitForPendingAdminTasks();

    // Then
    assertThat(metadataManager.refreshes).isEmpty();
  }

  @Test
  public void should_remove_node() {
    // When
    metadataManager.removeNode(ADDRESS1);
    waitForPendingAdminTasks();

    // Then
    assertThat(metadataManager.refreshes).hasSize(1);
    RemoveNodeRefresh refresh = (RemoveNodeRefresh) metadataManager.refreshes.get(0);
    assertThat(refresh.toRemove).isEqualTo(ADDRESS1);
  }

  @AfterMethod
  public void teardown() {
    adminEventLoopGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
  }

  private class TestMetadataManager extends MetadataManager {

    private List<MetadataRefresh> refreshes = new CopyOnWriteArrayList<>();

    public TestMetadataManager(InternalDriverContext context) {
      super(context);
    }

    @Override
    Void refresh(MetadataRefresh refresh) {
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
