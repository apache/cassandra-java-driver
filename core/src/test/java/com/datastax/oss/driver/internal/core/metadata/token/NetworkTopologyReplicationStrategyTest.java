/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata.token;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class NetworkTopologyReplicationStrategyTest {

  private static final String DC1 = "DC1";
  private static final String DC2 = "DC2";
  private static final String DC3 = "DC3";
  private static final String RACK11 = "RACK11";
  private static final String RACK12 = "RACK12";
  private static final String RACK21 = "RACK21";
  private static final String RACK22 = "RACK22";
  private static final String RACK31 = "RACK31";

  private static final Token TOKEN01 = new Murmur3Token(-9000000000000000000L);
  private static final Token TOKEN02 = new Murmur3Token(-8000000000000000000L);
  private static final Token TOKEN03 = new Murmur3Token(-7000000000000000000L);
  private static final Token TOKEN04 = new Murmur3Token(-6000000000000000000L);
  private static final Token TOKEN05 = new Murmur3Token(-5000000000000000000L);
  private static final Token TOKEN06 = new Murmur3Token(-4000000000000000000L);
  private static final Token TOKEN07 = new Murmur3Token(-3000000000000000000L);
  private static final Token TOKEN08 = new Murmur3Token(-2000000000000000000L);
  private static final Token TOKEN09 = new Murmur3Token(-1000000000000000000L);
  private static final Token TOKEN10 = new Murmur3Token(0L);
  private static final Token TOKEN11 = new Murmur3Token(1000000000000000000L);
  private static final Token TOKEN12 = new Murmur3Token(2000000000000000000L);
  private static final Token TOKEN13 = new Murmur3Token(3000000000000000000L);
  private static final Token TOKEN14 = new Murmur3Token(4000000000000000000L);
  private static final Token TOKEN15 = new Murmur3Token(5000000000000000000L);
  private static final Token TOKEN16 = new Murmur3Token(6000000000000000000L);
  private static final Token TOKEN17 = new Murmur3Token(7000000000000000000L);
  private static final Token TOKEN18 = new Murmur3Token(8000000000000000000L);
  private static final Token TOKEN19 = new Murmur3Token(9000000000000000000L);

  @Mock private Node node1, node2, node3, node4, node5, node6, node7, node8;

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  /** 4 tokens, 2 nodes in 2 DCs, RF = 1 in each DC. */
  @Test
  public void should_compute_for_simple_layout() {
    // Given
    List<Token> ring = ImmutableList.of(TOKEN01, TOKEN04, TOKEN14, TOKEN19);
    locate(node1, DC1, RACK11);
    locate(node2, DC2, RACK21);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.of(TOKEN01, node1, TOKEN04, node2, TOKEN14, node1, TOKEN19, node2);
    ReplicationStrategy strategy =
        new NetworkTopologyReplicationStrategy(ImmutableMap.of(DC1, "1", DC2, "1"), "test");

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    // Note: this also asserts the iteration order of the sets (unlike containsEntry(token, set))
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node2);
    assertThat(replicasByToken.get(TOKEN04)).containsExactly(node2, node1);
    assertThat(replicasByToken.get(TOKEN14)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN04));
  }

  /** 8 tokens, 4 nodes in 2 DCs in the same racks, RF = 1 in each DC. */
  @Test
  public void should_compute_for_simple_layout_with_multiple_nodes_per_rack() {
    // Given
    List<Token> ring =
        ImmutableList.of(TOKEN01, TOKEN03, TOKEN05, TOKEN07, TOKEN13, TOKEN15, TOKEN17, TOKEN19);
    locate(node1, DC1, RACK11);
    locate(node2, DC2, RACK21);
    locate(node3, DC1, RACK11);
    locate(node4, DC2, RACK21);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.<Token, Node>builder()
            .put(TOKEN01, node1)
            .put(TOKEN03, node2)
            .put(TOKEN05, node3)
            .put(TOKEN07, node4)
            .put(TOKEN13, node1)
            .put(TOKEN15, node2)
            .put(TOKEN17, node3)
            .put(TOKEN19, node4)
            .build();
    ReplicationStrategy strategy =
        new NetworkTopologyReplicationStrategy(ImmutableMap.of(DC1, "1", DC2, "1"), "test");

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node2);
    assertThat(replicasByToken.get(TOKEN03)).containsExactly(node2, node3);
    assertThat(replicasByToken.get(TOKEN05)).containsExactly(node3, node4);
    assertThat(replicasByToken.get(TOKEN07)).containsExactly(node4, node1);
    assertThat(replicasByToken.get(TOKEN13)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN15)).isSameAs(replicasByToken.get(TOKEN03));
    assertThat(replicasByToken.get(TOKEN17)).isSameAs(replicasByToken.get(TOKEN05));
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN07));
  }

  /** 6 tokens, 3 nodes in 3 DCs, RF = 1 in each DC. */
  @Test
  public void should_compute_for_simple_layout_with_3_dcs() {
    // Given
    List<Token> ring = ImmutableList.of(TOKEN01, TOKEN05, TOKEN09, TOKEN11, TOKEN15, TOKEN19);
    locate(node1, DC1, RACK11);
    locate(node2, DC2, RACK21);
    locate(node3, DC3, RACK31);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.<Token, Node>builder()
            .put(TOKEN01, node1)
            .put(TOKEN05, node2)
            .put(TOKEN09, node3)
            .put(TOKEN11, node1)
            .put(TOKEN15, node2)
            .put(TOKEN19, node3)
            .build();
    ReplicationStrategy strategy =
        new NetworkTopologyReplicationStrategy(
            ImmutableMap.of(DC1, "1", DC2, "1", DC3, "1"), "test");

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node2, node3);
    assertThat(replicasByToken.get(TOKEN05)).containsExactly(node2, node3, node1);
    assertThat(replicasByToken.get(TOKEN09)).containsExactly(node3, node1, node2);
    assertThat(replicasByToken.get(TOKEN11)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN15)).isSameAs(replicasByToken.get(TOKEN05));
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN09));
  }

  /** 10 tokens, 4 nodes in 2 DCs, RF = 2 in each DC, 1 node owns 4 tokens, the others only 2. */
  @Test
  public void should_compute_for_unbalanced_ring() {
    // Given
    List<Token> ring =
        ImmutableList.of(
            TOKEN01, TOKEN03, TOKEN05, TOKEN07, TOKEN09, TOKEN11, TOKEN13, TOKEN15, TOKEN17,
            TOKEN19);
    locate(node1, DC1, RACK11);
    locate(node2, DC2, RACK21);
    locate(node3, DC1, RACK11);
    locate(node4, DC2, RACK21);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.<Token, Node>builder()
            .put(TOKEN01, node1)
            .put(TOKEN03, node1)
            .put(TOKEN05, node2)
            .put(TOKEN07, node3)
            .put(TOKEN09, node4)
            .put(TOKEN11, node1)
            .put(TOKEN13, node1)
            .put(TOKEN15, node2)
            .put(TOKEN17, node3)
            .put(TOKEN19, node4)
            .build();
    ReplicationStrategy strategy =
        new NetworkTopologyReplicationStrategy(ImmutableMap.of(DC1, "2", DC2, "2"), "test");

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node2, node3, node4);
    assertThat(replicasByToken.get(TOKEN03)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN05)).containsExactly(node2, node3, node4, node1);
    assertThat(replicasByToken.get(TOKEN07)).containsExactly(node3, node4, node1, node2);
    assertThat(replicasByToken.get(TOKEN09)).containsExactly(node4, node1, node2, node3);
    assertThat(replicasByToken.get(TOKEN11)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN13)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN15)).isSameAs(replicasByToken.get(TOKEN05));
    assertThat(replicasByToken.get(TOKEN17)).isSameAs(replicasByToken.get(TOKEN07));
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN09));
    ;
  }

  /** 16 tokens, 8 nodes in 2 DCs with 2 per rack, RF = 2 in each DC. */
  @Test
  public void should_compute_with_multiple_racks_per_dc() {
    // Given
    List<Token> ring =
        ImmutableList.of(
            TOKEN01, TOKEN02, TOKEN03, TOKEN04, TOKEN05, TOKEN06, TOKEN07, TOKEN08, TOKEN12,
            TOKEN13, TOKEN14, TOKEN15, TOKEN16, TOKEN17, TOKEN18, TOKEN19);
    locate(node1, DC1, RACK11);
    locate(node2, DC2, RACK21);
    locate(node3, DC1, RACK12);
    locate(node4, DC2, RACK22);
    locate(node5, DC1, RACK11);
    locate(node6, DC2, RACK21);
    locate(node7, DC1, RACK12);
    locate(node8, DC2, RACK22);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.<Token, Node>builder()
            .put(TOKEN01, node1)
            .put(TOKEN02, node2)
            .put(TOKEN03, node3)
            .put(TOKEN04, node4)
            .put(TOKEN05, node5)
            .put(TOKEN06, node6)
            .put(TOKEN07, node7)
            .put(TOKEN08, node8)
            .put(TOKEN12, node1)
            .put(TOKEN13, node2)
            .put(TOKEN14, node3)
            .put(TOKEN15, node4)
            .put(TOKEN16, node5)
            .put(TOKEN17, node6)
            .put(TOKEN18, node7)
            .put(TOKEN19, node8)
            .build();
    ReplicationStrategy strategy =
        new NetworkTopologyReplicationStrategy(ImmutableMap.of(DC1, "2", DC2, "2"), "test");

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node2, node3, node4);
    assertThat(replicasByToken.get(TOKEN02)).containsExactly(node2, node3, node4, node5);
    assertThat(replicasByToken.get(TOKEN03)).containsExactly(node3, node4, node5, node6);
    assertThat(replicasByToken.get(TOKEN04)).containsExactly(node4, node5, node6, node7);
    assertThat(replicasByToken.get(TOKEN05)).containsExactly(node5, node6, node7, node8);
    assertThat(replicasByToken.get(TOKEN06)).containsExactly(node6, node7, node8, node1);
    assertThat(replicasByToken.get(TOKEN07)).containsExactly(node7, node8, node1, node2);
    assertThat(replicasByToken.get(TOKEN08)).containsExactly(node8, node1, node2, node3);
    assertThat(replicasByToken.get(TOKEN12)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN13)).isSameAs(replicasByToken.get(TOKEN02));
    assertThat(replicasByToken.get(TOKEN14)).isSameAs(replicasByToken.get(TOKEN03));
    assertThat(replicasByToken.get(TOKEN15)).isSameAs(replicasByToken.get(TOKEN04));
    assertThat(replicasByToken.get(TOKEN16)).isSameAs(replicasByToken.get(TOKEN05));
    assertThat(replicasByToken.get(TOKEN17)).isSameAs(replicasByToken.get(TOKEN06));
    assertThat(replicasByToken.get(TOKEN18)).isSameAs(replicasByToken.get(TOKEN07));
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN08));
  }

  /**
   * 16 tokens, 8 nodes in 2 DCs with 2 per rack, RF = 3 in each DC.
   *
   * <p>The nodes that are in the same rack occupy consecutive positions on the ring. We want to
   * reproduce the case where we hit the same rack when we look for the second replica of a DC; the
   * expected behavior is to skip the node and go to the next rack, and come back to the first rack
   * for the third replica.
   */
  @Test
  public void should_pick_dc_replicas_in_different_racks_first() {
    // Given
    List<Token> ring =
        ImmutableList.of(
            TOKEN01, TOKEN02, TOKEN03, TOKEN04, TOKEN05, TOKEN06, TOKEN07, TOKEN08, TOKEN12,
            TOKEN13, TOKEN14, TOKEN15, TOKEN16, TOKEN17, TOKEN18, TOKEN19);
    locate(node1, DC1, RACK11);
    locate(node2, DC2, RACK21);
    locate(node3, DC1, RACK11);
    locate(node4, DC2, RACK21);
    locate(node5, DC1, RACK12);
    locate(node6, DC2, RACK22);
    locate(node7, DC1, RACK12);
    locate(node8, DC2, RACK22);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.<Token, Node>builder()
            .put(TOKEN01, node1)
            .put(TOKEN02, node2)
            .put(TOKEN03, node3)
            .put(TOKEN04, node4)
            .put(TOKEN05, node5)
            .put(TOKEN06, node6)
            .put(TOKEN07, node7)
            .put(TOKEN08, node8)
            .put(TOKEN12, node1)
            .put(TOKEN13, node2)
            .put(TOKEN14, node3)
            .put(TOKEN15, node4)
            .put(TOKEN16, node5)
            .put(TOKEN17, node6)
            .put(TOKEN18, node7)
            .put(TOKEN19, node8)
            .build();
    ReplicationStrategy strategy =
        new NetworkTopologyReplicationStrategy(ImmutableMap.of(DC1, "3", DC2, "3"), "test");

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    assertThat(replicasByToken.get(TOKEN01))
        .containsExactly(node1, node2, node5, node3, node6, node4);
    assertThat(replicasByToken.get(TOKEN02))
        .containsExactly(node2, node3, node5, node6, node4, node7);
    assertThat(replicasByToken.get(TOKEN03))
        .containsExactly(node3, node4, node5, node6, node7, node8);
    assertThat(replicasByToken.get(TOKEN04))
        .containsExactly(node4, node5, node6, node8, node1, node7);
    assertThat(replicasByToken.get(TOKEN05))
        .containsExactly(node5, node6, node1, node7, node2, node8);
    assertThat(replicasByToken.get(TOKEN06))
        .containsExactly(node6, node7, node1, node2, node8, node3);
    assertThat(replicasByToken.get(TOKEN07))
        .containsExactly(node7, node8, node1, node2, node3, node4);
    assertThat(replicasByToken.get(TOKEN08))
        .containsExactly(node8, node1, node2, node4, node5, node3);
    assertThat(replicasByToken.get(TOKEN12)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN13)).isSameAs(replicasByToken.get(TOKEN02));
    assertThat(replicasByToken.get(TOKEN14)).isSameAs(replicasByToken.get(TOKEN03));
    assertThat(replicasByToken.get(TOKEN15)).isSameAs(replicasByToken.get(TOKEN04));
    assertThat(replicasByToken.get(TOKEN16)).isSameAs(replicasByToken.get(TOKEN05));
    assertThat(replicasByToken.get(TOKEN17)).isSameAs(replicasByToken.get(TOKEN06));
    assertThat(replicasByToken.get(TOKEN18)).isSameAs(replicasByToken.get(TOKEN07));
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN08));
  }

  /**
   * 16 tokens, 8 nodes in 2 DCs with 2 per rack, RF = 3 in each DC.
   *
   * <p>This is the same scenario as {@link #should_pick_dc_replicas_in_different_racks_first()},
   * except that each node owns consecutive tokens on the ring.
   */
  @Test
  public void should_pick_dc_replicas_in_different_racks_first_when_nodes_own_consecutive_tokens() {
    // When
    Map<Token, Set<Node>> replicasByToken = computeWithDifferentRacksAndConsecutiveTokens(3);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(16);
    assertThat(replicasByToken.get(TOKEN01))
        .containsExactly(node1, node5, node3, node2, node6, node4);
    assertThat(replicasByToken.get(TOKEN02)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN03))
        .containsExactly(node3, node5, node7, node2, node6, node4);
    assertThat(replicasByToken.get(TOKEN04)).isSameAs(replicasByToken.get(TOKEN03));
    assertThat(replicasByToken.get(TOKEN05))
        .containsExactly(node5, node2, node6, node4, node1, node7);
    assertThat(replicasByToken.get(TOKEN06)).isSameAs(replicasByToken.get(TOKEN05));
    assertThat(replicasByToken.get(TOKEN07))
        .containsExactly(node7, node2, node6, node4, node1, node3);
    assertThat(replicasByToken.get(TOKEN08)).isSameAs(replicasByToken.get(TOKEN07));
    assertThat(replicasByToken.get(TOKEN12))
        .containsExactly(node2, node6, node4, node1, node5, node3);
    assertThat(replicasByToken.get(TOKEN13)).isSameAs(replicasByToken.get(TOKEN12));
    assertThat(replicasByToken.get(TOKEN14))
        .containsExactly(node4, node6, node8, node1, node5, node3);
    assertThat(replicasByToken.get(TOKEN15)).isSameAs(replicasByToken.get(TOKEN14));
    assertThat(replicasByToken.get(TOKEN16))
        .containsExactly(node6, node1, node5, node3, node2, node8);
    assertThat(replicasByToken.get(TOKEN17)).isSameAs(replicasByToken.get(TOKEN16));
    assertThat(replicasByToken.get(TOKEN18))
        .containsExactly(node8, node1, node5, node3, node2, node4);
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN18));
  }

  /**
   * 16 tokens, 8 nodes in 2 DCs with 2 per rack, RF = 4 in each DC.
   *
   * <p>This is the same test as {@link
   * #should_pick_dc_replicas_in_different_racks_first_when_nodes_own_consecutive_tokens()}, except
   * for the replication factors.
   */
  @Test
  public void should_pick_dc_replicas_in_different_racks_first_when_all_nodes_contain_all_data() {
    // When
    Map<Token, Set<Node>> replicasByToken = computeWithDifferentRacksAndConsecutiveTokens(4);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(16);
    assertThat(replicasByToken.get(TOKEN01))
        .containsExactly(node1, node5, node3, node7, node2, node6, node4, node8);
    assertThat(replicasByToken.get(TOKEN02)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN03))
        .containsExactly(node3, node5, node7, node2, node6, node4, node8, node1);
    assertThat(replicasByToken.get(TOKEN04)).isSameAs(replicasByToken.get(TOKEN03));
    assertThat(replicasByToken.get(TOKEN05))
        .containsExactly(node5, node2, node6, node4, node8, node1, node7, node3);
    assertThat(replicasByToken.get(TOKEN06)).isSameAs(replicasByToken.get(TOKEN05));
    assertThat(replicasByToken.get(TOKEN07))
        .containsExactly(node7, node2, node6, node4, node8, node1, node3, node5);
    assertThat(replicasByToken.get(TOKEN08)).isSameAs(replicasByToken.get(TOKEN07));
    assertThat(replicasByToken.get(TOKEN12))
        .containsExactly(node2, node6, node4, node8, node1, node5, node3, node7);
    assertThat(replicasByToken.get(TOKEN13)).isSameAs(replicasByToken.get(TOKEN12));
    assertThat(replicasByToken.get(TOKEN14))
        .containsExactly(node4, node6, node8, node1, node5, node3, node7, node2);
    assertThat(replicasByToken.get(TOKEN15)).isSameAs(replicasByToken.get(TOKEN14));
    assertThat(replicasByToken.get(TOKEN16))
        .containsExactly(node6, node1, node5, node3, node7, node2, node8, node4);
    assertThat(replicasByToken.get(TOKEN17)).isSameAs(replicasByToken.get(TOKEN16));
    assertThat(replicasByToken.get(TOKEN18))
        .containsExactly(node8, node1, node5, node3, node7, node2, node4, node6);
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN18));
  }

  private Map<Token, Set<Node>> computeWithDifferentRacksAndConsecutiveTokens(
      int replicationFactor) {
    List<Token> ring =
        ImmutableList.of(
            TOKEN01, TOKEN02, TOKEN03, TOKEN04, TOKEN05, TOKEN06, TOKEN07, TOKEN08, TOKEN12,
            TOKEN13, TOKEN14, TOKEN15, TOKEN16, TOKEN17, TOKEN18, TOKEN19);
    locate(node1, DC1, RACK11);
    locate(node2, DC2, RACK21);
    locate(node3, DC1, RACK11);
    locate(node4, DC2, RACK21);
    locate(node5, DC1, RACK12);
    locate(node6, DC2, RACK22);
    locate(node7, DC1, RACK12);
    locate(node8, DC2, RACK22);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.<Token, Node>builder()
            .put(TOKEN01, node1)
            .put(TOKEN02, node1)
            .put(TOKEN03, node3)
            .put(TOKEN04, node3)
            .put(TOKEN05, node5)
            .put(TOKEN06, node5)
            .put(TOKEN07, node7)
            .put(TOKEN08, node7)
            .put(TOKEN12, node2)
            .put(TOKEN13, node2)
            .put(TOKEN14, node4)
            .put(TOKEN15, node4)
            .put(TOKEN16, node6)
            .put(TOKEN17, node6)
            .put(TOKEN18, node8)
            .put(TOKEN19, node8)
            .build();
    ReplicationStrategy strategy =
        new NetworkTopologyReplicationStrategy(
            ImmutableMap.of(
                DC1, Integer.toString(replicationFactor), DC2, Integer.toString(replicationFactor)),
            "test");

    return strategy.computeReplicasByToken(tokenToPrimary, ring);
  }

  /**
   * 18 tokens, 6 nodes in 2 DCs with 2 in rack 1 and 1 in rack 2, RF = 2 in each DC.
   *
   * <p>This is taken from a real-life cluster.
   */
  @Test
  public void should_compute_complex_layout() {
    // When
    Map<Token, Set<Node>> replicasByToken = computeComplexLayout(2);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(18);
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node5, node2, node6);
    assertThat(replicasByToken.get(TOKEN02)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN03)).containsExactly(node5, node3, node2, node6);
    assertThat(replicasByToken.get(TOKEN04)).containsExactly(node3, node5, node2, node6);
    assertThat(replicasByToken.get(TOKEN05)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN06)).containsExactly(node5, node2, node6, node3);
    assertThat(replicasByToken.get(TOKEN07)).containsExactly(node2, node6, node3, node5);
    assertThat(replicasByToken.get(TOKEN08)).containsExactly(node6, node3, node4, node5);
    assertThat(replicasByToken.get(TOKEN09)).containsExactly(node3, node4, node5, node6);
    assertThat(replicasByToken.get(TOKEN10)).containsExactly(node4, node5, node6, node3);
    assertThat(replicasByToken.get(TOKEN11)).containsExactly(node5, node4, node6, node3);
    assertThat(replicasByToken.get(TOKEN12)).containsExactly(node4, node6, node3, node5);
    assertThat(replicasByToken.get(TOKEN13)).isSameAs(replicasByToken.get(TOKEN12));
    assertThat(replicasByToken.get(TOKEN14)).isSameAs(replicasByToken.get(TOKEN07));
    assertThat(replicasByToken.get(TOKEN15)).containsExactly(node6, node3, node2, node5);
    assertThat(replicasByToken.get(TOKEN16)).containsExactly(node3, node2, node6, node5);
    assertThat(replicasByToken.get(TOKEN17)).containsExactly(node2, node6, node1, node5);
    assertThat(replicasByToken.get(TOKEN18)).containsExactly(node6, node1, node5, node2);
  }

  /**
   * 18 tokens, 6 nodes in 2 DCs with 2 in rack 1 and 1 in rack 2, RF = 4 in each DC.
   *
   * <p>This is the same test as {@link #should_compute_complex_layout()}, but with RF = 4, which is
   * too high for this cluster (it would require 8 nodes).
   */
  @Test
  public void should_compute_complex_layout_with_rf_too_high() {
    // When
    Map<Token, Set<Node>> replicasByToken = computeComplexLayout(4);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(18);
    assertThat(replicasByToken.get(TOKEN01))
        .containsExactly(node1, node5, node3, node2, node6, node4);
    assertThat(replicasByToken.get(TOKEN02)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN03))
        .containsExactly(node5, node3, node1, node2, node6, node4);
    assertThat(replicasByToken.get(TOKEN04))
        .containsExactly(node3, node5, node1, node2, node6, node4);
    assertThat(replicasByToken.get(TOKEN05))
        .containsExactly(node1, node5, node2, node6, node3, node4);
    assertThat(replicasByToken.get(TOKEN06))
        .containsExactly(node5, node2, node6, node3, node4, node1);
    assertThat(replicasByToken.get(TOKEN07))
        .containsExactly(node2, node6, node3, node4, node5, node1);
    assertThat(replicasByToken.get(TOKEN08))
        .containsExactly(node6, node3, node4, node5, node2, node1);
    assertThat(replicasByToken.get(TOKEN09))
        .containsExactly(node3, node4, node5, node6, node2, node1);
    assertThat(replicasByToken.get(TOKEN10))
        .containsExactly(node4, node5, node6, node2, node3, node1);
    assertThat(replicasByToken.get(TOKEN11))
        .containsExactly(node5, node4, node6, node2, node3, node1);
    assertThat(replicasByToken.get(TOKEN12))
        .containsExactly(node4, node6, node2, node3, node5, node1);
    assertThat(replicasByToken.get(TOKEN13)).isSameAs(replicasByToken.get(TOKEN12));
    assertThat(replicasByToken.get(TOKEN14))
        .containsExactly(node2, node6, node3, node5, node1, node4);
    assertThat(replicasByToken.get(TOKEN15))
        .containsExactly(node6, node3, node2, node5, node1, node4);
    assertThat(replicasByToken.get(TOKEN16))
        .containsExactly(node3, node2, node6, node5, node1, node4);
    assertThat(replicasByToken.get(TOKEN17))
        .containsExactly(node2, node6, node1, node5, node3, node4);
    assertThat(replicasByToken.get(TOKEN18))
        .containsExactly(node6, node1, node5, node3, node2, node4);
  }

  private Map<Token, Set<Node>> computeComplexLayout(int replicationFactor) {
    List<Token> ring =
        ImmutableList.of(
            TOKEN01, TOKEN02, TOKEN03, TOKEN04, TOKEN05, TOKEN06, TOKEN07, TOKEN08, TOKEN09,
            TOKEN10, TOKEN11, TOKEN12, TOKEN13, TOKEN14, TOKEN15, TOKEN16, TOKEN17, TOKEN18);
    locate(node1, DC1, RACK11);
    locate(node2, DC2, RACK21);
    locate(node3, DC1, RACK11);
    locate(node4, DC2, RACK21);
    locate(node5, DC1, RACK12);
    locate(node6, DC2, RACK22);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.<Token, Node>builder()
            .put(TOKEN01, node1)
            .put(TOKEN02, node1)
            .put(TOKEN03, node5)
            .put(TOKEN04, node3)
            .put(TOKEN05, node1)
            .put(TOKEN06, node5)
            .put(TOKEN07, node2)
            .put(TOKEN08, node6)
            .put(TOKEN09, node3)
            .put(TOKEN10, node4)
            .put(TOKEN11, node5)
            .put(TOKEN12, node4)
            .put(TOKEN13, node4)
            .put(TOKEN14, node2)
            .put(TOKEN15, node6)
            .put(TOKEN16, node3)
            .put(TOKEN17, node2)
            .put(TOKEN18, node6)
            .build();
    ReplicationStrategy strategy =
        new NetworkTopologyReplicationStrategy(
            ImmutableMap.of(
                DC1, Integer.toString(replicationFactor), DC2, Integer.toString(replicationFactor)),
            "test");

    return strategy.computeReplicasByToken(tokenToPrimary, ring);
  }

  /**
   * When the replication factors are invalid (user error) and a datacenter has a replication factor
   * that cannot be met, we want to quickly abort and move on to the next DC (instead of keeping
   * scanning the ring in vain, which results in quadratic complexity). We also log a warning to
   * give the user a chance to fix their settings.
   *
   * @see <a href="https://datastax-oss.atlassian.net/browse/JAVA-702">JAVA-702</a>
   * @see <a href="https://datastax-oss.atlassian.net/browse/JAVA-859">JAVA-859</a>
   */
  @Test
  public void should_abort_early_and_log_when_bad_replication_factor_cannot_be_met() {
    // Given
    List<Token> ring = ImmutableList.of(TOKEN01, TOKEN04, TOKEN14, TOKEN19);
    locate(node1, DC1, RACK11);
    locate(node2, DC2, RACK21);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.of(TOKEN01, node1, TOKEN04, node2, TOKEN14, node1, TOKEN19, node2);
    Logger logger = (Logger) LoggerFactory.getLogger(NetworkTopologyReplicationStrategy.class);
    logger.addAppender(appender);

    try {
      // When
      int traversedTokensForValidSettings =
          countTraversedTokens(ring, tokenToPrimary, ImmutableMap.of(DC1, "1", DC2, "1"));

      // Then
      // No logs:
      verify(appender, never()).doAppend(any(ILoggingEvent.class));

      // When
      int traversedTokensForInvalidSettings =
          countTraversedTokens(ring, tokenToPrimary, ImmutableMap.of(DC1, "1", DC2, "1", DC3, "1"));
      // Did not take more steps than the valid settings
      assertThat(traversedTokensForInvalidSettings).isEqualTo(traversedTokensForValidSettings);
      // Did log:
      verify(appender).doAppend(loggingEventCaptor.capture());
      ILoggingEvent log = loggingEventCaptor.getValue();
      assertThat(log.getLevel()).isEqualTo(Level.WARN);
      assertThat(log.getMessage()).contains("could not achieve replication factor");
    } finally {
      logger.detachAppender(appender);
    }
  }

  // Counts the number of steps on the ring for a particular computation
  private int countTraversedTokens(
      List<Token> ring,
      Map<Token, Node> tokenToPrimary,
      ImmutableMap<String, String> replicationConfig) {
    AtomicInteger count = new AtomicInteger();
    List<Token> ringSpy = spy(ring);
    when(ringSpy.get(anyInt()))
        .thenAnswer(
            invocation -> {
              count.incrementAndGet();
              return invocation.callRealMethod();
            });
    new NetworkTopologyReplicationStrategy(replicationConfig, "test")
        .computeReplicasByToken(tokenToPrimary, ringSpy);
    return count.get();
  }

  private void locate(Node node, String dc, String rack) {
    when(node.getDatacenter()).thenReturn(dc);
    when(node.getRack()).thenReturn(rack);
  }
}
