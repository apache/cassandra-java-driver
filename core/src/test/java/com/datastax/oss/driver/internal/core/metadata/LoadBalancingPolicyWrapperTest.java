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

import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy.DistanceReporter;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.never;

public class LoadBalancingPolicyWrapperTest {

  private DefaultNode node1;
  private DefaultNode node2;
  private DefaultNode node3;

  private Map<InetSocketAddress, Node> contactPointsMap;
  private Queue<Node> policysQueryPlan;

  @Mock private InternalDriverContext context;
  @Mock private LoadBalancingPolicy loadBalancingPolicy;
  private EventBus eventBus;
  @Mock private MetadataManager metadataManager;
  @Mock private Metadata metadata;

  private LoadBalancingPolicyWrapper wrapper;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    node1 = new DefaultNode(new InetSocketAddress("127.0.0.1", 9042));
    node2 = new DefaultNode(new InetSocketAddress("127.0.0.2", 9042));
    node3 = new DefaultNode(new InetSocketAddress("127.0.0.3", 9042));

    contactPointsMap =
        ImmutableMap.<InetSocketAddress, Node>builder()
            .put(node1.getConnectAddress(), node1)
            .put(node2.getConnectAddress(), node2)
            .build();
    Mockito.when(metadata.getNodes()).thenReturn(contactPointsMap);
    Mockito.when(metadataManager.getMetadata()).thenReturn(metadata);
    Mockito.when(context.metadataManager()).thenReturn(metadataManager);

    policysQueryPlan = Lists.newLinkedList(ImmutableList.of(node3, node2, node1));
    Mockito.when(loadBalancingPolicy.newQueryPlan()).thenReturn(policysQueryPlan);

    eventBus = Mockito.spy(new EventBus("test"));
    Mockito.when(context.eventBus()).thenReturn(eventBus);

    wrapper = new LoadBalancingPolicyWrapper(context, loadBalancingPolicy);
  }

  @Test
  public void should_build_query_plan_from_contact_points_before_init() {
    // When
    Queue<Node> queryPlan = wrapper.newQueryPlan();

    // Then
    Mockito.verify(loadBalancingPolicy, never()).newQueryPlan();
    assertThat(queryPlan).containsOnlyElementsOf(contactPointsMap.values());
  }

  @Test
  public void should_fetch_query_plan_from_policy_after_init() {
    // Given
    wrapper.init();
    Mockito.verify(loadBalancingPolicy).init(anySet(), any(DistanceReporter.class));

    // When
    Queue<Node> queryPlan = wrapper.newQueryPlan();

    // Then
    Mockito.verify(loadBalancingPolicy).newQueryPlan();
    assertThat(queryPlan).isEqualTo(policysQueryPlan);
  }

  @Test
  public void should_init_policy_with_up_or_unknown_nodes() {
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
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Set<Node>> captor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(loadBalancingPolicy).init(captor.capture(), any(DistanceReporter.class));
    Set<Node> initNodes = captor.getValue();
    assertThat(initNodes).containsOnly(node1, node2);
  }

  @Test
  public void should_propagate_distance_from_policy() {
    // Given
    wrapper.init();
    ArgumentCaptor<DistanceReporter> captor = ArgumentCaptor.forClass(DistanceReporter.class);
    Mockito.verify(loadBalancingPolicy).init(anySet(), captor.capture());
    DistanceReporter distanceReporter = captor.getValue();

    // When
    distanceReporter.setDistance(node1, NodeDistance.LOCAL);

    // Then
    Mockito.verify(eventBus).fire(new DistanceEvent(NodeDistance.LOCAL, node1));
  }

  @Test
  public void should_not_propagate_node_states_to_policy_until_init() {
    // When
    eventBus.fire(NodeStateEvent.changed(NodeState.UNKNOWN, NodeState.UP, node1));

    // Then
    Mockito.verify(loadBalancingPolicy, never()).onUp(node1);
  }

  @Test
  public void should_propagate_node_states_to_policy_after_init() {
    // Given
    wrapper.init();

    // When
    eventBus.fire(NodeStateEvent.changed(NodeState.UNKNOWN, NodeState.UP, node1));

    // Then
    Mockito.verify(loadBalancingPolicy).onUp(node1);
  }

  @Test
  public void should_accumulate_events_during_init_and_replay() throws InterruptedException {
    // Given
    // Hack to obtain concurrency: the main thread blocks in init, while another thread fires an
    // event on the bus
    CountDownLatch eventLatch = new CountDownLatch(1);
    CountDownLatch initLatch = new CountDownLatch(1);
    Answer mockInit =
        i -> {
          eventLatch.countDown();
          initLatch.await(500, TimeUnit.MILLISECONDS);
          return null;
        };
    Mockito.doAnswer(mockInit)
        .when(loadBalancingPolicy)
        .init(anySet(), any(DistanceReporter.class));

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
    Mockito.verify(loadBalancingPolicy).onDown(node1);
    if (thread.isAlive()) {
      // thread still completing - sleep to allow thread to complete.
      Thread.sleep(500);
    }
    assertThat(thread.isAlive()).isFalse();
  }
}
