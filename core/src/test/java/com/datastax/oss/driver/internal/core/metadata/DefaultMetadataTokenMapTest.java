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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultReplicationStrategyFactory;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMetadataTokenMapTest {

  // Simulate the simplest setup possible for a functional token map. We're not testing the token
  // map itself, only how the metadata interacts with it.
  private static final String TOKEN1 = "-9000000000000000000";
  private static final String TOKEN2 = "9000000000000000000";
  private static final Node NODE1 = mockNode(TOKEN1);
  private static final Node NODE2 = mockNode(TOKEN2);
  private static final CqlIdentifier KEYSPACE_NAME = CqlIdentifier.fromInternal("ks");
  private static final KeyspaceMetadata KEYSPACE =
      mockKeyspace(
          KEYSPACE_NAME,
          ImmutableMap.of(
              "class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1"));

  @Mock private InternalDriverContext context;
  @Mock private ChannelFactory channelFactory;

  @Before
  public void setup() {
    when(context.getChannelFactory()).thenReturn(channelFactory);
    DefaultReplicationStrategyFactory replicationStrategyFactory =
        new DefaultReplicationStrategyFactory(context);
    when(context.getReplicationStrategyFactory()).thenReturn(replicationStrategyFactory);
  }

  @Test
  public void should_not_build_token_map_when_initializing_with_contact_points() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(
            ImmutableMap.of(NODE1.getHostId(), NODE1), Collections.emptyMap(), null, null);
    assertThat(contactPointsMetadata.getTokenMap()).isNotPresent();
  }

  @Test
  public void should_build_minimal_token_map_on_first_refresh() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(
            ImmutableMap.of(NODE1.getHostId(), NODE1), Collections.emptyMap(), null, null);
    DefaultMetadata firstRefreshMetadata =
        contactPointsMetadata.withNodes(
            ImmutableMap.of(NODE1.getHostId(), NODE1),
            true,
            true,
            new Murmur3TokenFactory(),
            context);
    assertThat(firstRefreshMetadata.getTokenMap().get().getTokenRanges()).hasSize(1);
  }

  @Test
  public void should_not_build_token_map_when_disabled() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(
            ImmutableMap.of(NODE1.getHostId(), NODE1), Collections.emptyMap(), null, null);
    DefaultMetadata firstRefreshMetadata =
        contactPointsMetadata.withNodes(
            ImmutableMap.of(NODE1.getHostId(), NODE1),
            false,
            true,
            new Murmur3TokenFactory(),
            context);
    assertThat(firstRefreshMetadata.getTokenMap()).isNotPresent();
  }

  @Test
  public void should_stay_empty_on_first_refresh_if_partitioner_missing() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(
            ImmutableMap.of(NODE1.getHostId(), NODE1), Collections.emptyMap(), null, null);
    DefaultMetadata firstRefreshMetadata =
        contactPointsMetadata.withNodes(
            ImmutableMap.of(NODE1.getHostId(), NODE1), true, true, null, context);
    assertThat(firstRefreshMetadata.getTokenMap()).isNotPresent();
  }

  @Test
  public void should_update_minimal_token_map_if_new_node_and_still_no_schema() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(
            ImmutableMap.of(NODE1.getHostId(), NODE1), Collections.emptyMap(), null, null);
    DefaultMetadata firstRefreshMetadata =
        contactPointsMetadata.withNodes(
            ImmutableMap.of(NODE1.getHostId(), NODE1),
            true,
            true,
            new Murmur3TokenFactory(),
            context);
    DefaultMetadata secondRefreshMetadata =
        firstRefreshMetadata.withNodes(
            ImmutableMap.of(NODE1.getHostId(), NODE1, NODE2.getHostId(), NODE2),
            true,
            false,
            null,
            context);
    assertThat(secondRefreshMetadata.getTokenMap().get().getTokenRanges()).hasSize(2);
  }

  @Test
  public void should_update_token_map_when_schema_changes() {
    DefaultMetadata contactPointsMetadata =
        new DefaultMetadata(
            ImmutableMap.of(NODE1.getHostId(), NODE1), Collections.emptyMap(), null, null);
    DefaultMetadata firstRefreshMetadata =
        contactPointsMetadata.withNodes(
            ImmutableMap.of(NODE1.getHostId(), NODE1),
            true,
            true,
            new Murmur3TokenFactory(),
            context);
    DefaultMetadata schemaRefreshMetadata =
        firstRefreshMetadata.withSchema(ImmutableMap.of(KEYSPACE_NAME, KEYSPACE), true, context);
    assertThat(schemaRefreshMetadata.getTokenMap().get().getTokenRanges(KEYSPACE_NAME, NODE1))
        .isNotEmpty();
  }

  private static DefaultNode mockNode(String token) {
    DefaultNode node = mock(DefaultNode.class);
    when(node.getHostId()).thenReturn(UUID.randomUUID());
    when(node.getRawTokens()).thenReturn(ImmutableSet.of(token));
    return node;
  }

  private static KeyspaceMetadata mockKeyspace(
      CqlIdentifier name, Map<String, String> replicationConfig) {
    KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
    when(keyspace.getName()).thenReturn(name);
    when(keyspace.getReplication()).thenReturn(replicationConfig);
    return keyspace;
  }
}
