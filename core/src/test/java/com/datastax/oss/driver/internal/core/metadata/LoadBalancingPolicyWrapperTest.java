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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy.DistanceReporter;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class LoadBalancingPolicyWrapperTest {

  private DefaultNode node1;
  private DefaultNode node2;
  private DefaultNode node3;

  private Map<InetSocketAddress, Node> contactPointsMap;
  private Queue<Node> defaultPolicysQueryPlan;

  @Mock private InternalDriverContext context;
  @Mock private LoadBalancingPolicy policy1;
  @Mock private LoadBalancingPolicy policy2;
  @Mock private LoadBalancingPolicy policy3;
  private EventBus eventBus;
  @Mock private MetadataManager metadataManager;
  @Mock private Metadata metadata;
  @Mock protected MetricsFactory metricsFactory;
  @Captor private ArgumentCaptor<Map<InetSocketAddress, Node>> initNodesCaptor;

  private LoadBalancingPolicyWrapper wrapper;

  @Before
  public void setup() {
    Mockito.when(context.metricsFactory()).thenReturn(metricsFactory);

    node1 = new DefaultNode(new InetSocketAddress("127.0.0.1", 9042), context);
    node2 = new DefaultNode(new InetSocketAddress("127.0.0.2", 9042), context);
    node3 = new DefaultNode(new InetSocketAddress("127.0.0.3", 9042), context);

    contactPointsMap =
        ImmutableMap.<InetSocketAddress, Node>builder()
            .put(node1.getConnectAddress(), node1)
            .put(node2.getConnectAddress(), node2)
            .build();
    Mockito.when(metadata.getNodes()).thenReturn(contactPointsMap);
    Mockito.when(metadataManager.getMetadata()).thenReturn(metadata);
    Mockito.when(metadataManager.getContactPoints()).thenReturn(contactPointsMap.keySet());
    Mockito.when(context.metadataManager()).thenReturn(metadataManager);

    defaultPolicysQueryPlan = Lists.newLinkedList(ImmutableList.of(node3, node2, node1));
    Mockito.when(policy1.newQueryPlan(null, null)).thenReturn(defaultPolicysQueryPlan);

    eventBus = Mockito.spy(new EventBus("test"));
    Mockito.when(context.eventBus()).thenReturn(eventBus);

    wrapper =
        new LoadBalancingPolicyWrapper(
            context,
            ImmutableMap.of(
                DriverConfigProfile.DEFAULT_NAME,
                policy1,
                "profile1",
                policy1,
                "profile2",
                policy2,
                "profile3",
                policy3));
  }

  @Test
  public void should_build_query_plan_from_contact_points_before_init() {
    // When
    Queue<Node> queryPlan = wrapper.newQueryPlan();

    // Then
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      Mockito.verify(policy, never()).newQueryPlan(null, null);
    }
    assertThat(queryPlan).containsOnlyElementsOf(contactPointsMap.values());
  }

  @Test
  public void should_fetch_query_plan_from_policy_after_init() {
    // Given
    wrapper.init();
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      Mockito.verify(policy)
          .init(anyMap(), any(DistanceReporter.class), eq(contactPointsMap.keySet()));
    }

    // When
    Queue<Node> queryPlan = wrapper.newQueryPlan();

    // Then
    // no-arg newQueryPlan() uses the default profile
    Mockito.verify(policy1).newQueryPlan(null, null);
    assertThat(queryPlan).isEqualTo(defaultPolicysQueryPlan);
  }

  @Test
  public void should_init_policies_with_up_or_unknown_nodes() {
    // Given
    node1.state = NodeState.UP;
    node2.state = NodeState.UNKNOWN;
    node3.state = NodeState.DOWN;
    Map<InetSocketAddress, Node> contactPointsMap2 =
        ImmutableMap.<InetSocketAddress, Node>builder()
            .put(node1.getConnectAddress(), node1)
            .put(node2.getConnectAddress(), node2)
            .put(node3.getConnectAddress(), node3)
            .build();
    Mockito.when(metadata.getNodes()).thenReturn(contactPointsMap2);

    // When
    wrapper.init();

    // Then
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      Mockito.verify(policy)
          .init(
              initNodesCaptor.capture(),
              any(DistanceReporter.class),
              eq(contactPointsMap.keySet()));
      Map<InetSocketAddress, Node> initNodes = initNodesCaptor.getValue();
      assertThat(initNodes.values()).containsOnly(node1, node2);
    }
  }

  @Test
  public void should_propagate_distances_from_policies() {
    // Given
    wrapper.init();
    ArgumentCaptor<DistanceReporter> captor1 = ArgumentCaptor.forClass(DistanceReporter.class);
    Mockito.verify(policy1).init(anyMap(), captor1.capture(), eq(contactPointsMap.keySet()));
    DistanceReporter distanceReporter1 = captor1.getValue();
    ArgumentCaptor<DistanceReporter> captor2 = ArgumentCaptor.forClass(DistanceReporter.class);
    Mockito.verify(policy2).init(anyMap(), captor2.capture(), eq(contactPointsMap.keySet()));
    DistanceReporter distanceReporter2 = captor1.getValue();
    ArgumentCaptor<DistanceReporter> captor3 = ArgumentCaptor.forClass(DistanceReporter.class);
    Mockito.verify(policy3).init(anyMap(), captor3.capture(), eq(contactPointsMap.keySet()));
    DistanceReporter distanceReporter3 = captor3.getValue();

    InOrder inOrder = Mockito.inOrder(eventBus);

    // When
    distanceReporter1.setDistance(node1, NodeDistance.REMOTE);

    // Then
    // first event defines the distance
    inOrder.verify(eventBus).fire(new DistanceEvent(NodeDistance.REMOTE, node1));

    // When
    distanceReporter2.setDistance(node1, NodeDistance.REMOTE);

    // Then
    // event is ignored if the node is already at this distance
    inOrder.verify(eventBus, times(0)).fire(any(DistanceEvent.class));

    // When
    distanceReporter2.setDistance(node1, NodeDistance.LOCAL);

    // Then
    // event is applied if it sets a smaller distance
    inOrder.verify(eventBus).fire(new DistanceEvent(NodeDistance.LOCAL, node1));

    // When
    distanceReporter3.setDistance(node1, NodeDistance.IGNORED);

    // Then
    // event is ignored if the node is already at a closer distance
    inOrder.verify(eventBus, times(0)).fire(any(DistanceEvent.class));
  }

  @Test
  public void should_not_propagate_node_states_to_policies_until_init() {
    // When
    eventBus.fire(NodeStateEvent.changed(NodeState.UNKNOWN, NodeState.UP, node1));

    // Then
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      Mockito.verify(policy, never()).onUp(node1);
    }
  }

  @Test
  public void should_propagate_node_states_to_policies_after_init() {
    // Given
    wrapper.init();

    // When
    eventBus.fire(NodeStateEvent.changed(NodeState.UNKNOWN, NodeState.UP, node1));

    // Then
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      Mockito.verify(policy).onUp(node1);
    }
  }

  @Test
  public void should_accumulate_events_during_init_and_replay() throws InterruptedException {
    // Given
    // Hack to obtain concurrency: the main thread blocks in init, while another thread fires an
    // event on the bus
    CountDownLatch eventLatch = new CountDownLatch(3);
    CountDownLatch initLatch = new CountDownLatch(1);
    Answer mockInit =
        i -> {
          eventLatch.countDown();
          initLatch.await(500, TimeUnit.MILLISECONDS);
          return null;
        };
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      Mockito.doAnswer(mockInit)
          .when(policy)
          .init(anyMap(), any(DistanceReporter.class), eq(contactPointsMap.keySet()));
    }

    // When
    Runnable runnable =
        () -> {
          try {
            eventLatch.await(500, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          eventBus.fire(NodeStateEvent.changed(NodeState.UNKNOWN, NodeState.DOWN, node1));
          initLatch.countDown();
        };
    Thread thread = new Thread(runnable);
    thread.start();
    wrapper.init();

    // Then
    // wait for init launch to signal that runnable is complete.
    initLatch.await(500, TimeUnit.MILLISECONDS);
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      Mockito.verify(policy).onDown(node1);
    }
    if (thread.isAlive()) {
      // thread still completing - sleep to allow thread to complete.
      Thread.sleep(500);
    }
    assertThat(thread.isAlive()).isFalse();
  }
}
