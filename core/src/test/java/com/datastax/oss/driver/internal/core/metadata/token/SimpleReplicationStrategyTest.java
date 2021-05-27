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
package com.datastax.oss.driver.internal.core.metadata.token;

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SimpleReplicationStrategyTest {

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

  @Mock private Node node1, node2, node3, node4, node5, node6;

  /** 4 tokens, 2 nodes, RF = 2. */
  @Test
  public void should_compute_for_simple_layout() {
    // Given
    List<Token> ring = ImmutableList.of(TOKEN01, TOKEN06, TOKEN14, TOKEN19);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.of(TOKEN01, node1, TOKEN06, node2, TOKEN14, node1, TOKEN19, node2);
    SimpleReplicationStrategy strategy = new SimpleReplicationStrategy(new ReplicationFactor(2));

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    // Note: this also asserts the iteration order of the sets (unlike containsEntry(token, set))
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node2);
    assertThat(replicasByToken.get(TOKEN06)).containsExactly(node2, node1);
    assertThat(replicasByToken.get(TOKEN14)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN06));
  }

  /** 4 tokens, 2 nodes owning 2 consecutive tokens each, RF = 2. */
  @Test
  public void should_compute_when_nodes_own_consecutive_tokens() {
    // Given
    List<Token> ring = ImmutableList.of(TOKEN01, TOKEN06, TOKEN14, TOKEN19);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.of(TOKEN01, node1, TOKEN06, node1, TOKEN14, node2, TOKEN19, node2);
    SimpleReplicationStrategy strategy = new SimpleReplicationStrategy(new ReplicationFactor(2));

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node2);
    assertThat(replicasByToken.get(TOKEN06)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN14)).containsExactly(node2, node1);
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN14));
  }

  /** 4 tokens, 1 node owns 3 of them, RF = 2. */
  @Test
  public void should_compute_when_ring_unbalanced() {
    // Given
    List<Token> ring = ImmutableList.of(TOKEN01, TOKEN06, TOKEN14, TOKEN19);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.of(TOKEN01, node1, TOKEN06, node1, TOKEN14, node2, TOKEN19, node1);
    SimpleReplicationStrategy strategy = new SimpleReplicationStrategy(new ReplicationFactor(2));

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node2);
    assertThat(replicasByToken.get(TOKEN06)).containsExactly(node1, node2);
    assertThat(replicasByToken.get(TOKEN14)).containsExactly(node2, node1);
    assertThat(replicasByToken.get(TOKEN19)).containsExactly(node1, node2);
  }

  /** 4 tokens, 2 nodes, RF = 6 (too large, should be <= number of nodes). */
  @Test
  public void should_compute_when_replication_factor_is_larger_than_cluster_size() {
    // Given
    List<Token> ring = ImmutableList.of(TOKEN01, TOKEN06, TOKEN14, TOKEN19);
    Map<Token, Node> tokenToPrimary =
        ImmutableMap.of(TOKEN01, node1, TOKEN06, node2, TOKEN14, node1, TOKEN19, node2);
    SimpleReplicationStrategy strategy = new SimpleReplicationStrategy(new ReplicationFactor(6));

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node2);
    assertThat(replicasByToken.get(TOKEN06)).containsExactly(node2, node1);
    assertThat(replicasByToken.get(TOKEN14)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN19)).isSameAs(replicasByToken.get(TOKEN06));
  }

  @Test
  public void should_compute_for_complex_layout() {
    // Given
    List<Token> ring =
        ImmutableList.<Token>builder()
            .add(TOKEN01)
            .add(TOKEN02)
            .add(TOKEN03)
            .add(TOKEN04)
            .add(TOKEN05)
            .add(TOKEN06)
            .add(TOKEN07)
            .add(TOKEN08)
            .add(TOKEN09)
            .add(TOKEN10)
            .add(TOKEN11)
            .add(TOKEN12)
            .add(TOKEN13)
            .add(TOKEN14)
            .add(TOKEN15)
            .add(TOKEN16)
            .add(TOKEN17)
            .add(TOKEN18)
            .build();
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

    SimpleReplicationStrategy strategy = new SimpleReplicationStrategy(new ReplicationFactor(3));

    // When
    Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);

    // Then
    assertThat(replicasByToken.keySet().size()).isEqualTo(ring.size());
    assertThat(replicasByToken.get(TOKEN01)).containsExactly(node1, node5, node3);
    assertThat(replicasByToken.get(TOKEN02)).isSameAs(replicasByToken.get(TOKEN01));
    assertThat(replicasByToken.get(TOKEN03)).containsExactly(node5, node3, node1);
    assertThat(replicasByToken.get(TOKEN04)).containsExactly(node3, node1, node5);
    assertThat(replicasByToken.get(TOKEN05)).containsExactly(node1, node5, node2);
    assertThat(replicasByToken.get(TOKEN06)).containsExactly(node5, node2, node6);
    assertThat(replicasByToken.get(TOKEN07)).containsExactly(node2, node6, node3);
    assertThat(replicasByToken.get(TOKEN08)).containsExactly(node6, node3, node4);
    assertThat(replicasByToken.get(TOKEN09)).containsExactly(node3, node4, node5);
    assertThat(replicasByToken.get(TOKEN10)).containsExactly(node4, node5, node2);
    assertThat(replicasByToken.get(TOKEN11)).containsExactly(node5, node4, node2);
    assertThat(replicasByToken.get(TOKEN12)).containsExactly(node4, node2, node6);
    assertThat(replicasByToken.get(TOKEN13)).isSameAs(replicasByToken.get(TOKEN12));
    assertThat(replicasByToken.get(TOKEN14)).isSameAs(replicasByToken.get(TOKEN07));
    assertThat(replicasByToken.get(TOKEN15)).containsExactly(node6, node3, node2);
    assertThat(replicasByToken.get(TOKEN16)).containsExactly(node3, node2, node6);
    assertThat(replicasByToken.get(TOKEN17)).containsExactly(node2, node6, node1);
    assertThat(replicasByToken.get(TOKEN18)).containsExactly(node6, node1, node5);
  }
}
