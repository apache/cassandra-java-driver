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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultTokenMapTest {

  private static final String DC1 = "DC1";
  private static final String DC2 = "DC2";
  private static final String RACK1 = "RACK1";
  private static final String RACK2 = "RACK2";

  private static final CqlIdentifier KS1 = CqlIdentifier.fromInternal("ks1");
  private static final CqlIdentifier KS2 = CqlIdentifier.fromInternal("ks2");

  private static final TokenFactory TOKEN_FACTORY = new Murmur3TokenFactory();

  private static final String TOKEN1 = "-9000000000000000000";
  private static final String TOKEN2 = "-6000000000000000000";
  private static final String TOKEN3 = "4000000000000000000";
  private static final String TOKEN4 = "9000000000000000000";
  private static final TokenRange RANGE12 = range(TOKEN1, TOKEN2);
  private static final TokenRange RANGE23 = range(TOKEN2, TOKEN3);
  private static final TokenRange RANGE34 = range(TOKEN3, TOKEN4);
  private static final TokenRange RANGE41 = range(TOKEN4, TOKEN1);
  private static final TokenRange FULL_RING =
      range(TOKEN_FACTORY.minToken(), TOKEN_FACTORY.minToken());

  // Some random routing keys that land in the ranges above (they were generated manually)
  private static ByteBuffer ROUTING_KEY12 = TypeCodecs.BIGINT.encode(2L, DefaultProtocolVersion.V3);
  private static ByteBuffer ROUTING_KEY23 = TypeCodecs.BIGINT.encode(0L, DefaultProtocolVersion.V3);
  private static ByteBuffer ROUTING_KEY34 = TypeCodecs.BIGINT.encode(1L, DefaultProtocolVersion.V3);
  private static ByteBuffer ROUTING_KEY41 =
      TypeCodecs.BIGINT.encode(99L, DefaultProtocolVersion.V3);

  private static final ImmutableMap<String, String> REPLICATE_ON_BOTH_DCS =
      ImmutableMap.of(
          "class", "org.apache.cassandra.locator.NetworkTopologyStrategy", DC1, "1", DC2, "1");
  private static final ImmutableMap<String, String> REPLICATE_ON_DC1 =
      ImmutableMap.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", DC1, "1");

  @Mock private InternalDriverContext context;
  private ReplicationStrategyFactory replicationStrategyFactory;

  @Before
  public void setup() {
    replicationStrategyFactory = new DefaultReplicationStrategyFactory(context);
  }

  @Test
  public void should_build_token_map() {
    // Given
    Node node1 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN1));
    Node node2 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN2));
    Node node3 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN3));
    Node node4 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN4));
    List<Node> nodes = ImmutableList.of(node1, node2, node3, node4);
    List<KeyspaceMetadata> keyspaces =
        ImmutableList.of(
            mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS), mockKeyspace(KS2, REPLICATE_ON_DC1));

    // When
    DefaultTokenMap tokenMap =
        DefaultTokenMap.build(nodes, keyspaces, TOKEN_FACTORY, replicationStrategyFactory, "test");

    // Then
    assertThat(tokenMap.getTokenRanges()).containsExactly(RANGE12, RANGE23, RANGE34, RANGE41);

    // For KS1, each node gets its primary range, plus the one of the previous node in the other DC
    assertThat(tokenMap.getTokenRanges(KS1, node1)).containsOnly(RANGE41, RANGE34);
    assertThat(tokenMap.getTokenRanges(KS1, node2)).containsOnly(RANGE12, RANGE41);
    assertThat(tokenMap.getTokenRanges(KS1, node3)).containsOnly(RANGE23, RANGE12);
    assertThat(tokenMap.getTokenRanges(KS1, node4)).containsOnly(RANGE34, RANGE23);

    assertThat(tokenMap.getReplicas(KS1, RANGE12)).containsOnly(node2, node3);
    assertThat(tokenMap.getReplicas(KS1, RANGE23)).containsOnly(node3, node4);
    assertThat(tokenMap.getReplicas(KS1, RANGE34)).containsOnly(node1, node4);
    assertThat(tokenMap.getReplicas(KS1, RANGE41)).containsOnly(node1, node2);

    assertThat(tokenMap.getReplicas(KS1, ROUTING_KEY12)).containsOnly(node2, node3);
    assertThat(tokenMap.getReplicas(KS1, ROUTING_KEY23)).containsOnly(node3, node4);
    assertThat(tokenMap.getReplicas(KS1, ROUTING_KEY34)).containsOnly(node1, node4);
    assertThat(tokenMap.getReplicas(KS1, ROUTING_KEY41)).containsOnly(node1, node2);

    // KS2 is only replicated on DC1
    assertThat(tokenMap.getTokenRanges(KS2, node1)).containsOnly(RANGE41, RANGE34);
    assertThat(tokenMap.getTokenRanges(KS2, node3)).containsOnly(RANGE23, RANGE12);
    assertThat(tokenMap.getTokenRanges(KS2, node2)).isEmpty();
    assertThat(tokenMap.getTokenRanges(KS2, node4)).isEmpty();

    assertThat(tokenMap.getReplicas(KS2, RANGE12)).containsOnly(node3);
    assertThat(tokenMap.getReplicas(KS2, RANGE23)).containsOnly(node3);
    assertThat(tokenMap.getReplicas(KS2, RANGE34)).containsOnly(node1);
    assertThat(tokenMap.getReplicas(KS2, RANGE41)).containsOnly(node1);

    assertThat(tokenMap.getReplicas(KS2, ROUTING_KEY12)).containsOnly(node3);
    assertThat(tokenMap.getReplicas(KS2, ROUTING_KEY23)).containsOnly(node3);
    assertThat(tokenMap.getReplicas(KS2, ROUTING_KEY34)).containsOnly(node1);
    assertThat(tokenMap.getReplicas(KS2, ROUTING_KEY41)).containsOnly(node1);
  }

  @Test
  public void should_build_token_map_with_single_node() {
    // Given
    Node node1 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN1));
    List<Node> nodes = ImmutableList.of(node1);
    List<KeyspaceMetadata> keyspaces =
        ImmutableList.of(
            mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS), mockKeyspace(KS2, REPLICATE_ON_DC1));

    // When
    DefaultTokenMap tokenMap =
        DefaultTokenMap.build(nodes, keyspaces, TOKEN_FACTORY, replicationStrategyFactory, "test");

    // Then
    assertThat(tokenMap.getTokenRanges()).containsExactly(FULL_RING);

    assertThat(tokenMap.getTokenRanges(KS1, node1)).containsOnly(FULL_RING);
    assertThat(tokenMap.getReplicas(KS1, FULL_RING)).containsOnly(node1);
    assertThat(tokenMap.getReplicas(KS1, ROUTING_KEY12)).containsOnly(node1);
    assertThat(tokenMap.getReplicas(KS1, ROUTING_KEY23)).containsOnly(node1);
    assertThat(tokenMap.getReplicas(KS1, ROUTING_KEY34)).containsOnly(node1);
    assertThat(tokenMap.getReplicas(KS1, ROUTING_KEY41)).containsOnly(node1);

    assertThat(tokenMap.getTokenRanges(KS2, node1)).containsOnly(FULL_RING);
    assertThat(tokenMap.getReplicas(KS2, FULL_RING)).containsOnly(node1);
    assertThat(tokenMap.getReplicas(KS2, ROUTING_KEY12)).containsOnly(node1);
    assertThat(tokenMap.getReplicas(KS2, ROUTING_KEY23)).containsOnly(node1);
    assertThat(tokenMap.getReplicas(KS2, ROUTING_KEY34)).containsOnly(node1);
    assertThat(tokenMap.getReplicas(KS2, ROUTING_KEY41)).containsOnly(node1);
  }

  @Test
  public void should_refresh_when_keyspace_replication_has_not_changed() {
    // Given
    Node node1 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN1));
    Node node2 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN2));
    Node node3 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN3));
    Node node4 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN4));
    List<Node> nodes = ImmutableList.of(node1, node2, node3, node4);
    List<KeyspaceMetadata> oldKeyspaces =
        ImmutableList.of(
            mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS), mockKeyspace(KS2, REPLICATE_ON_DC1));
    DefaultTokenMap oldTokenMap =
        DefaultTokenMap.build(
            nodes, oldKeyspaces, TOKEN_FACTORY, replicationStrategyFactory, "test");

    // When
    // The schema gets refreshed, but no keyspaces are created or dropped, and the replication
    // settings do not change (since we mock everything it looks the same here, but it could be a
    // new table, etc).
    List<KeyspaceMetadata> newKeyspaces =
        ImmutableList.of(
            mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS), mockKeyspace(KS2, REPLICATE_ON_DC1));
    DefaultTokenMap newTokenMap =
        oldTokenMap.refresh(nodes, newKeyspaces, replicationStrategyFactory);

    // Then
    // Nothing was recomputed
    assertThat(newTokenMap.tokenRanges).isSameAs(oldTokenMap.tokenRanges);
    assertThat(newTokenMap.tokenRangesByPrimary).isSameAs(oldTokenMap.tokenRangesByPrimary);
    assertThat(newTokenMap.replicationConfigs).isSameAs(oldTokenMap.replicationConfigs);
    assertThat(newTokenMap.keyspaceMaps).isSameAs(oldTokenMap.keyspaceMaps);
  }

  @Test
  public void should_refresh_when_new_keyspace_with_existing_replication() {
    // Given
    Node node1 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN1));
    Node node2 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN2));
    Node node3 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN3));
    Node node4 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN4));
    List<Node> nodes = ImmutableList.of(node1, node2, node3, node4);
    List<KeyspaceMetadata> oldKeyspaces =
        ImmutableList.of(mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS));
    DefaultTokenMap oldTokenMap =
        DefaultTokenMap.build(
            nodes, oldKeyspaces, TOKEN_FACTORY, replicationStrategyFactory, "test");
    assertThat(oldTokenMap.keyspaceMaps).containsOnlyKeys(REPLICATE_ON_BOTH_DCS);

    // When
    List<KeyspaceMetadata> newKeyspaces =
        ImmutableList.of(
            mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS), mockKeyspace(KS2, REPLICATE_ON_BOTH_DCS));
    DefaultTokenMap newTokenMap =
        oldTokenMap.refresh(nodes, newKeyspaces, replicationStrategyFactory);

    // Then
    assertThat(newTokenMap.tokenRanges).isSameAs(oldTokenMap.tokenRanges);
    assertThat(newTokenMap.tokenRangesByPrimary).isSameAs(oldTokenMap.tokenRangesByPrimary);
    assertThat(newTokenMap.keyspaceMaps).isEqualTo(oldTokenMap.keyspaceMaps);
    assertThat(newTokenMap.replicationConfigs)
        .hasSize(2)
        .containsEntry(KS1, REPLICATE_ON_BOTH_DCS)
        .containsEntry(KS2, REPLICATE_ON_BOTH_DCS);
  }

  @Test
  public void should_refresh_when_new_keyspace_with_new_replication() {
    // Given
    Node node1 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN1));
    Node node2 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN2));
    Node node3 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN3));
    Node node4 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN4));
    List<Node> nodes = ImmutableList.of(node1, node2, node3, node4);
    List<KeyspaceMetadata> oldKeyspaces =
        ImmutableList.of(mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS));
    DefaultTokenMap oldTokenMap =
        DefaultTokenMap.build(
            nodes, oldKeyspaces, TOKEN_FACTORY, replicationStrategyFactory, "test");
    assertThat(oldTokenMap.keyspaceMaps).containsOnlyKeys(REPLICATE_ON_BOTH_DCS);

    // When
    List<KeyspaceMetadata> newKeyspaces =
        ImmutableList.of(
            mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS), mockKeyspace(KS2, REPLICATE_ON_DC1));
    DefaultTokenMap newTokenMap =
        oldTokenMap.refresh(nodes, newKeyspaces, replicationStrategyFactory);

    // Then
    assertThat(newTokenMap.tokenRanges).isSameAs(oldTokenMap.tokenRanges);
    assertThat(newTokenMap.tokenRangesByPrimary).isSameAs(oldTokenMap.tokenRangesByPrimary);
    assertThat(newTokenMap.keyspaceMaps).containsOnlyKeys(REPLICATE_ON_BOTH_DCS, REPLICATE_ON_DC1);
    assertThat(newTokenMap.replicationConfigs)
        .hasSize(2)
        .containsEntry(KS1, REPLICATE_ON_BOTH_DCS)
        .containsEntry(KS2, REPLICATE_ON_DC1);
  }

  @Test
  public void should_refresh_when_dropped_keyspace_with_replication_still_used() {
    // Given
    Node node1 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN1));
    Node node2 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN2));
    Node node3 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN3));
    Node node4 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN4));
    List<Node> nodes = ImmutableList.of(node1, node2, node3, node4);
    List<KeyspaceMetadata> oldKeyspaces =
        ImmutableList.of(
            mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS), mockKeyspace(KS2, REPLICATE_ON_BOTH_DCS));
    DefaultTokenMap oldTokenMap =
        DefaultTokenMap.build(
            nodes, oldKeyspaces, TOKEN_FACTORY, replicationStrategyFactory, "test");
    assertThat(oldTokenMap.keyspaceMaps).containsOnlyKeys(REPLICATE_ON_BOTH_DCS);

    // When
    List<KeyspaceMetadata> newKeyspaces =
        ImmutableList.of(mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS));
    DefaultTokenMap newTokenMap =
        oldTokenMap.refresh(nodes, newKeyspaces, replicationStrategyFactory);

    // Then
    assertThat(newTokenMap.tokenRanges).isSameAs(oldTokenMap.tokenRanges);
    assertThat(newTokenMap.tokenRangesByPrimary).isSameAs(oldTokenMap.tokenRangesByPrimary);
    assertThat(newTokenMap.keyspaceMaps).containsOnlyKeys(REPLICATE_ON_BOTH_DCS);
    assertThat(newTokenMap.replicationConfigs).hasSize(1).containsEntry(KS1, REPLICATE_ON_BOTH_DCS);
  }

  @Test
  public void should_refresh_when_dropped_keyspace_with_replication_not_used_anymore() {
    // Given
    Node node1 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN1));
    Node node2 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN2));
    Node node3 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN3));
    Node node4 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN4));
    List<Node> nodes = ImmutableList.of(node1, node2, node3, node4);
    List<KeyspaceMetadata> oldKeyspaces =
        ImmutableList.of(
            mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS), mockKeyspace(KS2, REPLICATE_ON_DC1));
    DefaultTokenMap oldTokenMap =
        DefaultTokenMap.build(
            nodes, oldKeyspaces, TOKEN_FACTORY, replicationStrategyFactory, "test");
    assertThat(oldTokenMap.keyspaceMaps).containsOnlyKeys(REPLICATE_ON_BOTH_DCS, REPLICATE_ON_DC1);

    // When
    List<KeyspaceMetadata> newKeyspaces =
        ImmutableList.of(mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS));
    DefaultTokenMap newTokenMap =
        oldTokenMap.refresh(nodes, newKeyspaces, replicationStrategyFactory);

    // Then
    assertThat(newTokenMap.tokenRanges).isSameAs(oldTokenMap.tokenRanges);
    assertThat(newTokenMap.tokenRangesByPrimary).isSameAs(oldTokenMap.tokenRangesByPrimary);
    assertThat(newTokenMap.keyspaceMaps).containsOnlyKeys(REPLICATE_ON_BOTH_DCS);
    assertThat(newTokenMap.replicationConfigs).hasSize(1).containsEntry(KS1, REPLICATE_ON_BOTH_DCS);
  }

  @Test
  public void should_refresh_when_updated_keyspace_with_different_replication() {
    // Given
    Node node1 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN1));
    Node node2 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN2));
    Node node3 = mockNode(DC1, RACK1, ImmutableSet.of(TOKEN3));
    Node node4 = mockNode(DC2, RACK2, ImmutableSet.of(TOKEN4));
    List<Node> nodes = ImmutableList.of(node1, node2, node3, node4);
    List<KeyspaceMetadata> oldKeyspaces =
        ImmutableList.of(
            mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS), mockKeyspace(KS2, REPLICATE_ON_DC1));
    DefaultTokenMap oldTokenMap =
        DefaultTokenMap.build(
            nodes, oldKeyspaces, TOKEN_FACTORY, replicationStrategyFactory, "test");
    assertThat(oldTokenMap.keyspaceMaps).containsOnlyKeys(REPLICATE_ON_BOTH_DCS, REPLICATE_ON_DC1);

    // When
    List<KeyspaceMetadata> newKeyspaces =
        ImmutableList.of(
            mockKeyspace(KS1, REPLICATE_ON_BOTH_DCS), mockKeyspace(KS2, REPLICATE_ON_BOTH_DCS));
    DefaultTokenMap newTokenMap =
        oldTokenMap.refresh(nodes, newKeyspaces, replicationStrategyFactory);

    // Then
    assertThat(newTokenMap.tokenRanges).isSameAs(oldTokenMap.tokenRanges);
    assertThat(newTokenMap.tokenRangesByPrimary).isSameAs(oldTokenMap.tokenRangesByPrimary);
    assertThat(newTokenMap.keyspaceMaps).containsOnlyKeys(REPLICATE_ON_BOTH_DCS);
    assertThat(newTokenMap.replicationConfigs)
        .hasSize(2)
        .containsEntry(KS1, REPLICATE_ON_BOTH_DCS)
        .containsEntry(KS2, REPLICATE_ON_BOTH_DCS);
  }

  private DefaultNode mockNode(String dc, String rack, Set<String> tokens) {
    DefaultNode node = mock(DefaultNode.class);
    when(node.getDatacenter()).thenReturn(dc);
    when(node.getRack()).thenReturn(rack);
    when(node.getRawTokens()).thenReturn(tokens);
    return node;
  }

  private KeyspaceMetadata mockKeyspace(CqlIdentifier name, Map<String, String> replicationConfig) {
    KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
    when(keyspace.getName()).thenReturn(name);
    when(keyspace.getReplication()).thenReturn(replicationConfig);
    return keyspace;
  }

  private static TokenRange range(String start, String end) {
    return range(TOKEN_FACTORY.parse(start), TOKEN_FACTORY.parse(end));
  }

  private static TokenRange range(Token startToken, Token endToken) {
    return new Murmur3TokenRange((Murmur3Token) startToken, (Murmur3Token) endToken);
  }
}
