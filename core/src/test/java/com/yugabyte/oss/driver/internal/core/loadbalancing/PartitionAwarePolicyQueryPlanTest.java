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
package com.yugabyte.oss.driver.internal.core.loadbalancing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.yugabyte.oss.driver.api.core.DefaultPartitionMetadata;
import com.yugabyte.oss.driver.api.core.TableSplitMetadata;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

// TODO fix unnecessary stubbing of config option in parent class (and stop using "silent" runner)
@RunWith(MockitoJUnitRunner.Silent.class)
public class PartitionAwarePolicyQueryPlanTest extends LoadBalancingPolicyTestBase {

  protected static final ByteBuffer ROUTING_KEY = Bytes.fromHexString("0xdeadbeef");

  @Mock protected BoundStatement boundStatement;
  @Mock protected PreparedStatement preparedStatement;
  @Mock protected ColumnDefinitions variableDefinitions;
  @Mock protected ColumnDefinition pkDefinition;
  @Mock protected DataType pkType;
  @Mock protected DefaultSession session;
  @Mock protected Metadata metadata;
  @Mock protected DefaultPartitionMetadata partitionMetadata;
  @Mock protected TableSplitMetadata tableSplitMetadata;

  protected static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");
  protected static final CqlIdentifier TABLE = CqlIdentifier.fromInternal("tbl");
  protected PartitionAwarePolicy policy;

  @Before
  @Override
  public void setup() {
    super.setup();

    when(node1.getState()).thenReturn(NodeState.UP);
    when(node2.getState()).thenReturn(NodeState.UP);
    when(node3.getState()).thenReturn(NodeState.UP);

    when(defaultProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("QUORUM");
    policy = createAndInitPolicy();
  }

  @Test
  public void should_use_leader_at_quorum_cl() {
    // Given
    when(node1.getDatacenter()).thenReturn("dc1");
    when(node2.getDatacenter()).thenReturn("dc1");
    when(node3.getDatacenter()).thenReturn("dc1");
    when(node1.getDistance()).thenReturn(NodeDistance.LOCAL);
    when(node2.getDistance()).thenReturn(NodeDistance.LOCAL);
    when(node3.getDistance()).thenReturn(NodeDistance.LOCAL);

    given(boundStatement.getPreparedStatement()).willReturn(preparedStatement);
    given(boundStatement.getConsistencyLevel()).willReturn(ConsistencyLevel.YB_STRONG);
    given(preparedStatement.getQuery()).willReturn("select * from tbl where pk = ?");
    given(preparedStatement.getVariableDefinitions()).willReturn(variableDefinitions);
    given(variableDefinitions.size()).willReturn(1);
    given(variableDefinitions.get(anyInt())).willReturn(pkDefinition);
    given(pkDefinition.getType()).willReturn(pkType);
    given(pkType.getProtocolCode()).willReturn(ProtocolConstants.DataType.INT);
    given(preparedStatement.getPartitionKeyIndices()).willReturn(Arrays.asList(0));
    java.nio.ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(1);
    given(boundStatement.getBytesUnsafe(anyInt())).willReturn(bb);
    given(pkDefinition.getKeyspace()).willReturn(KEYSPACE);
    given(pkDefinition.getTable()).willReturn(TABLE);
    given(session.getMetadata()).willReturn(metadata);
    given(metadata.getDefaultPartitionMetadata())
        .willReturn(Optional.ofNullable(partitionMetadata));
    given(partitionMetadata.getTableSplitMetadata(anyString(), anyString()))
        .willReturn(tableSplitMetadata);
    given(tableSplitMetadata.getHosts(anyInt())).willReturn(ImmutableList.of(node1, node2, node3));
    // When
    assertThat(policy.newQueryPlan(boundStatement, session)).containsExactly(node1, node2, node3);
    assertThat(policy.newQueryPlan(boundStatement, session)).containsExactly(node1, node2, node3);
    assertThat(policy.newQueryPlan(boundStatement, session)).containsExactly(node1, node2, node3);

    // Leader change
    given(tableSplitMetadata.getHosts(anyInt())).willReturn(ImmutableList.of(node3, node2, node1));
    // When
    assertThat(policy.newQueryPlan(boundStatement, session)).containsExactly(node3, node2, node1);
    assertThat(policy.newQueryPlan(boundStatement, session)).containsExactly(node3, node2, node1);
    assertThat(policy.newQueryPlan(boundStatement, session)).containsExactly(node3, node2, node1);
  }

  @Test
  public void should_round_robin_at_cl_one_single_dc() {
    when(node1.getDatacenter()).thenReturn("dc1");
    when(node2.getDatacenter()).thenReturn("dc1");
    when(node3.getDatacenter()).thenReturn("dc1");
    when(node1.getDistance()).thenReturn(NodeDistance.LOCAL);
    when(node2.getDistance()).thenReturn(NodeDistance.LOCAL);
    when(node3.getDistance()).thenReturn(NodeDistance.LOCAL);
    // Given
    given(boundStatement.getPreparedStatement()).willReturn(preparedStatement);
    given(boundStatement.getConsistencyLevel()).willReturn(ConsistencyLevel.ONE);
    given(preparedStatement.getQuery()).willReturn("select * from tbl where pk = ?");
    given(preparedStatement.getVariableDefinitions()).willReturn(variableDefinitions);
    given(variableDefinitions.size()).willReturn(1);
    given(variableDefinitions.get(anyInt())).willReturn(pkDefinition);
    given(pkDefinition.getType()).willReturn(pkType);
    given(pkType.getProtocolCode()).willReturn(ProtocolConstants.DataType.INT);
    given(preparedStatement.getPartitionKeyIndices()).willReturn(Arrays.asList(0));
    java.nio.ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(1);
    given(boundStatement.getBytesUnsafe(anyInt())).willReturn(bb);
    given(pkDefinition.getKeyspace()).willReturn(KEYSPACE);
    given(pkDefinition.getTable()).willReturn(TABLE);
    given(session.getMetadata()).willReturn(metadata);
    given(metadata.getDefaultPartitionMetadata())
        .willReturn(Optional.ofNullable(partitionMetadata));
    given(partitionMetadata.getTableSplitMetadata(anyString(), anyString()))
        .willReturn(tableSplitMetadata);
    given(tableSplitMetadata.getHosts(anyInt())).willReturn(ImmutableList.of(node1, node2, node3));
    // When TODO CL ONE randomly shuffles the node list. need to figure out if there's a way to test
    // properly
    assertThat(policy.newQueryPlan(boundStatement, session)).contains(node1, node2, node3);
    assertThat(policy.newQueryPlan(boundStatement, session)).contains(node3, node1, node2);
    assertThat(policy.newQueryPlan(boundStatement, session)).contains(node1, node2, node3);
  }

  @Test
  public void should_pick_local_at_cl_one_three_dc() {
    when(node1.getDatacenter()).thenReturn("dc1");
    when(node2.getDatacenter()).thenReturn("dc2");
    when(node3.getDatacenter()).thenReturn("dc3");
    when(node1.getDistance()).thenReturn(NodeDistance.LOCAL);
    when(node2.getDistance()).thenReturn(NodeDistance.REMOTE);
    when(node3.getDistance()).thenReturn(NodeDistance.REMOTE);
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(true);
    when(defaultProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn("dc1");

    // Given
    given(boundStatement.getPreparedStatement()).willReturn(preparedStatement);
    given(boundStatement.getConsistencyLevel()).willReturn(ConsistencyLevel.ONE);
    given(preparedStatement.getQuery()).willReturn("select * from tbl where pk = ?");
    given(preparedStatement.getVariableDefinitions()).willReturn(variableDefinitions);
    given(variableDefinitions.size()).willReturn(1);
    given(variableDefinitions.get(anyInt())).willReturn(pkDefinition);
    given(pkDefinition.getType()).willReturn(pkType);
    given(pkType.getProtocolCode()).willReturn(ProtocolConstants.DataType.INT);
    given(preparedStatement.getPartitionKeyIndices()).willReturn(Arrays.asList(0));
    java.nio.ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(1);
    given(boundStatement.getBytesUnsafe(anyInt())).willReturn(bb);
    given(pkDefinition.getKeyspace()).willReturn(KEYSPACE);
    given(pkDefinition.getTable()).willReturn(TABLE);
    given(session.getMetadata()).willReturn(metadata);
    given(metadata.getDefaultPartitionMetadata())
        .willReturn(Optional.ofNullable(partitionMetadata));
    given(partitionMetadata.getTableSplitMetadata(anyString(), anyString()))
        .willReturn(tableSplitMetadata);
    given(tableSplitMetadata.getHosts(anyInt())).willReturn(ImmutableList.of(node1, node2, node3));
    // When
    assertThat(policy.newQueryPlan(boundStatement, session)).containsExactly(node1);
    assertThat(policy.newQueryPlan(boundStatement, session)).containsExactly(node1);
    assertThat(policy.newQueryPlan(boundStatement, session)).containsExactly(node1);
  }

  protected PartitionAwarePolicy createAndInitPolicy() {
    PartitionAwarePolicy policy = new PartitionAwarePolicy(context, "default");
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1,
            UUID.randomUUID(), node2,
            UUID.randomUUID(), node3),
        distanceReporter);
    return policy;
  }
}
