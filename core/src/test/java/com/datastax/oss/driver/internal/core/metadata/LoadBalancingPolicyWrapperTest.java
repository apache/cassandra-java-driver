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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class LoadBalancingPolicyWrapperTest {

  private DefaultNode node1;
  private DefaultNode node2;
  private DefaultNode node3;

  private Map<UUID, Node> allNodes;
  private Set<DefaultNode> contactPoints;
  private Queue<Node> defaultPolicysQueryPlan;

  @Mock private InternalDriverContext context;
  @Mock private LoadBalancingPolicy policy1;
  @Mock private LoadBalancingPolicy policy2;
  @Mock private LoadBalancingPolicy policy3;
  private EventBus eventBus;
  @Mock private MetadataManager metadataManager;
  @Mock private Metadata metadata;
  @Mock protected MetricsFactory metricsFactory;
  @Captor private ArgumentCaptor<Map<UUID, Node>> initNodesCaptor;

  private LoadBalancingPolicyWrapper wrapper;

  @Before
  public void setup() {
    when(context.getMetricsFactory()).thenReturn(metricsFactory);

    node1 = TestNodeFactory.newNode(1, context);
    node2 = TestNodeFactory.newNode(2, context);
    node3 = TestNodeFactory.newNode(3, context);

    contactPoints = ImmutableSet.of(node1, node2);
    allNodes =
        ImmutableMap.of(
            node1.getHostId(), node1, node2.getHostId(), node2, node3.getHostId(), node3);
    when(metadataManager.getMetadata()).thenReturn(metadata);
    when(metadata.getNodes()).thenReturn(allNodes);
    when(metadataManager.getContactPoints()).thenReturn(contactPoints);
    when(context.getMetadataManager()).thenReturn(metadataManager);

    defaultPolicysQueryPlan = Lists.newLinkedList(ImmutableList.of(node3, node2, node1));
    when(policy1.newQueryPlan(null, null)).thenReturn(defaultPolicysQueryPlan);

    eventBus = spy(new EventBus("test"));
    when(context.getEventBus()).thenReturn(eventBus);

    wrapper =
        new LoadBalancingPolicyWrapper(
            context,
            ImmutableMap.of(
                DriverExecutionProfile.DEFAULT_NAME,
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
      verify(policy, never()).newQueryPlan(null, null);
    }
    assertThat(queryPlan).containsOnlyElementsOf(contactPoints);
  }

  @Test
  public void should_fetch_query_plan_from_policy_after_init() {
    // Given
    wrapper.init();
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      verify(policy).init(anyMap(), any(DistanceReporter.class));
    }

    // When
    Queue<Node> queryPlan = wrapper.newQueryPlan();

    // Then
    // no-arg newQueryPlan() uses the default profile
    verify(policy1).newQueryPlan(null, null);
    assertThat(queryPlan).isEqualTo(defaultPolicysQueryPlan);
  }

  @Test
  public void should_init_policies_with_all_nodes() {
    // Given
    node1.state = NodeState.UP;
    node2.state = NodeState.UNKNOWN;
    node3.state = NodeState.DOWN;

    // When
    wrapper.init();

    // Then
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      verify(policy).init(initNodesCaptor.capture(), any(DistanceReporter.class));
      Map<UUID, Node> initNodes = initNodesCaptor.getValue();
      assertThat(initNodes.values()).containsOnly(node1, node2, node3);
    }
  }

  @Test
  public void should_propagate_distances_from_policies() {
    // Given
    wrapper.init();
    ArgumentCaptor<DistanceReporter> captor1 = ArgumentCaptor.forClass(DistanceReporter.class);
    verify(policy1).init(anyMap(), captor1.capture());
    DistanceReporter distanceReporter1 = captor1.getValue();
    ArgumentCaptor<DistanceReporter> captor2 = ArgumentCaptor.forClass(DistanceReporter.class);
    verify(policy2).init(anyMap(), captor2.capture());
    DistanceReporter distanceReporter2 = captor1.getValue();
    ArgumentCaptor<DistanceReporter> captor3 = ArgumentCaptor.forClass(DistanceReporter.class);
    verify(policy3).init(anyMap(), captor3.capture());
    DistanceReporter distanceReporter3 = captor3.getValue();

    InOrder inOrder = inOrder(eventBus);

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
      verify(policy, never()).onUp(node1);
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
      verify(policy).onUp(node1);
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
      doAnswer(mockInit).when(policy).init(anyMap(), any(DistanceReporter.class));
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
      verify(policy).onDown(node1);
    }
    if (thread.isAlive()) {
      // thread still completing - sleep to allow thread to complete.
      Thread.sleep(500);
    }
    assertThat(thread.isAlive()).isFalse();
  }
}
