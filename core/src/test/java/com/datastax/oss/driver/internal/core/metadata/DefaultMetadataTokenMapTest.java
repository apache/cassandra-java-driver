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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.net.InetSocketAddress;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

public class DefaultMetadataTokenMapTest {

  // Simulate the simplest setup possible for a functional token map. We're not testing the token
  // map itself, only how the metadata interacts with it.
  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);
  private static final String TOKEN1 = "-9000000000000000000";
  private static final String TOKEN2 = "9000000000000000000";
  private static final TokenFactory TOKEN_FACTORY = new Murmur3TokenFactory();
  private static final Node NODE1 = mockNode(TOKEN1);
  private static final Node NODE2 = mockNode(TOKEN2);
  private static final CqlIdentifier KEYSPACE_NAME = CqlIdentifier.fromInternal("ks");
  private static final KeyspaceMetadata KEYSPACE =
      mockKeyspace(
          KEYSPACE_NAME,
          ImmutableMap.of(
              "class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1"));

  @Test
  public void should_not_build_token_map_when_initializing_with_contact_points() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, NODE1), "test");
    assertThat(contactPointsMetadata.getTokenMap()).isNotPresent();
  }

  @Test
  public void should_build_minimal_token_map_on_first_refresh() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, NODE1), "test");
    DefaultMetadata firstRefreshMetadata =
        contactPointsMetadata.withNodes(
            ImmutableMap.of(ADDRESS1, NODE1), true, true, new Murmur3TokenFactory());
    assertThat(firstRefreshMetadata.getTokenMap().get().getTokenRanges()).hasSize(1);
  }

  @Test
  public void should_not_build_token_map_when_disabled() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, NODE1), "test");
    DefaultMetadata firstRefreshMetadata =
        contactPointsMetadata.withNodes(
            ImmutableMap.of(ADDRESS1, NODE1), false, true, new Murmur3TokenFactory());
    assertThat(firstRefreshMetadata.getTokenMap()).isNotPresent();
  }

  @Test
  public void should_stay_empty_on_first_refresh_if_partitioner_missing() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, NODE1), "test");
    DefaultMetadata firstRefreshMetadata =
        contactPointsMetadata.withNodes(ImmutableMap.of(ADDRESS1, NODE1), true, true, null);
    assertThat(firstRefreshMetadata.getTokenMap()).isNotPresent();
  }

  @Test
  public void should_update_minimal_token_map_if_new_node_and_still_no_schema() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, NODE1), "test");
    DefaultMetadata firstRefreshMetadata =
        contactPointsMetadata.withNodes(
            ImmutableMap.of(ADDRESS1, NODE1), true, true, new Murmur3TokenFactory());
    DefaultMetadata secondRefreshMetadata =
        firstRefreshMetadata.withNodes(
            ImmutableMap.of(ADDRESS1, NODE1, ADDRESS2, NODE2), true, false, null);
    assertThat(secondRefreshMetadata.getTokenMap().get().getTokenRanges()).hasSize(2);
  }

  @Test
  public void should_update_token_map_when_schema_changes() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, NODE1), "test");
    DefaultMetadata firstRefreshMetadata =
        contactPointsMetadata.withNodes(
            ImmutableMap.of(ADDRESS1, NODE1), true, true, new Murmur3TokenFactory());
    DefaultMetadata schemaRefreshMetadata =
        firstRefreshMetadata.withSchema(ImmutableMap.of(KEYSPACE_NAME, KEYSPACE), true);
    assertThat(schemaRefreshMetadata.getTokenMap().get().getTokenRanges(KEYSPACE_NAME, NODE1))
        .isNotEmpty();
  }

  private static DefaultNode mockNode(String token) {
    DefaultNode node = Mockito.mock(DefaultNode.class);
    Mockito.when(node.getRawTokens()).thenReturn(ImmutableSet.of(token));
    return node;
  }

  private static KeyspaceMetadata mockKeyspace(
      CqlIdentifier name, Map<String, String> replicationConfig) {
    KeyspaceMetadata keyspace = Mockito.mock(KeyspaceMetadata.class);
    Mockito.when(keyspace.getName()).thenReturn(name);
    Mockito.when(keyspace.getReplication()).thenReturn(replicationConfig);
    return keyspace;
  }
}
