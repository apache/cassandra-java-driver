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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.loadbalancing;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

// TODO fix unnecessary stubbing of config option in parent class (and stop using "silent" runner)
@RunWith(MockitoJUnitRunner.Silent.class)
public class BasicLoadBalancingPolicyQueryPlanTest extends LoadBalancingPolicyTestBase {

  protected static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");
  protected static final ByteBuffer ROUTING_KEY = Bytes.fromHexString("0xdeadbeef");

  @Mock protected Request request;
  @Mock protected DefaultSession session;
  @Mock protected Metadata metadata;
  @Mock protected TokenMap tokenMap;
  @Mock protected Token routingToken;

  protected BasicLoadBalancingPolicy policy;

  @Before
  @Override
  public void setup() {
    super.setup();
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    when(metadataManager.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenAnswer(invocation -> Optional.of(this.tokenMap));

    policy = createAndInitPolicy();
  }

  @Test
  public void should_use_round_robin_when_no_request() {
    // Given
    request = null;

    // When
    assertRoundRobinQueryPlans();

    // Then
    then(tokenMap).should(never()).getReplicas(any(CqlIdentifier.class), any(Token.class));
    then(tokenMap)
        .should(never())
        .getReplicas(any(CqlIdentifier.class), isNull(), any(ByteBuffer.class));
  }

  @Test
  public void should_use_round_robin_when_no_session() {
    // Given
    session = null;

    // When
    assertRoundRobinQueryPlans();

    // Then
    then(request).should(never()).getRoutingKey();
    then(request).should(never()).getRoutingToken();
    then(tokenMap).should(never()).getReplicas(any(CqlIdentifier.class), any(Token.class));
    then(tokenMap)
        .should(never())
        .getReplicas(any(CqlIdentifier.class), isNull(), any(ByteBuffer.class));
  }

  @Test
  public void should_use_round_robin_when_request_has_no_routing_keyspace() {
    // By default from Mockito:
    assertThat(request.getKeyspace()).isNull();
    assertThat(request.getRoutingKeyspace()).isNull();

    assertRoundRobinQueryPlans();

    then(request).should(never()).getRoutingKey();
    then(request).should(never()).getRoutingToken();
    then(tokenMap).should(never()).getReplicas(any(CqlIdentifier.class), any(Token.class));
    then(tokenMap)
        .should(never())
        .getReplicas(any(CqlIdentifier.class), isNull(), any(ByteBuffer.class));
  }

  @Test
  public void should_use_round_robin_when_request_has_no_routing_key_or_token() {
    when(request.getRoutingKeyspace()).thenReturn(KEYSPACE);
    assertThat(request.getRoutingKey()).isNull();
    assertThat(request.getRoutingToken()).isNull();

    assertRoundRobinQueryPlans();

    then(tokenMap).should(never()).getReplicas(any(CqlIdentifier.class), any(Token.class));
    then(tokenMap)
        .should(never())
        .getReplicas(any(CqlIdentifier.class), isNull(), any(ByteBuffer.class));
  }

  @Test
  public void should_use_round_robin_when_token_map_absent() {
    when(metadata.getTokenMap()).thenReturn(Optional.empty());

    assertRoundRobinQueryPlans();

    then(tokenMap).should(never()).getReplicas(any(CqlIdentifier.class), any(Token.class));
    then(tokenMap)
        .should(never())
        .getReplicas(any(CqlIdentifier.class), isNull(), any(ByteBuffer.class));
  }

  @Test
  public void
      should_use_round_robin_when_token_map_returns_no_replicas_using_request_keyspace_and_routing_key() {
    when(request.getRoutingKeyspace()).thenReturn(KEYSPACE);
    when(request.getRoutingKey()).thenReturn(ROUTING_KEY);
    when(tokenMap.getReplicas(KEYSPACE, null, ROUTING_KEY)).thenReturn(Collections.emptySet());

    assertRoundRobinQueryPlans();

    then(tokenMap).should(atLeast(1)).getReplicas(KEYSPACE, null, ROUTING_KEY);
  }

  @Test
  public void
      should_use_round_robin_when_token_map_returns_no_replicas_using_session_keyspace_and_routing_key() {
    // Given
    given(request.getKeyspace()).willReturn(null);
    given(request.getRoutingKeyspace()).willReturn(null);
    given(session.getKeyspace()).willReturn(Optional.of(KEYSPACE));
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, null, ROUTING_KEY)).willReturn(emptySet());
    // When
    assertRoundRobinQueryPlans();
    // Then
    then(tokenMap).should(atLeast(1)).getReplicas(KEYSPACE, null, ROUTING_KEY);
  }

  @Test
  public void
      should_use_round_robin_when_token_map_returns_no_replicas_using_request_keyspace_and_routing_token() {
    // Given
    given(request.getKeyspace()).willReturn(null);
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingToken()).willReturn(routingToken);
    given(tokenMap.getReplicas(KEYSPACE, routingToken)).willReturn(emptySet());
    // When
    assertRoundRobinQueryPlans();
    // Then
    then(tokenMap).should(atLeast(1)).getReplicas(KEYSPACE, routingToken);
  }

  @Test
  public void should_use_round_robin_and_log_error_when_request_throws() {
    // Given
    given(request.getKeyspace()).willThrow(new NullPointerException());
    // When
    policy.newQueryPlan(request, session);
    // Then
    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .contains("Unexpected error while trying to compute query plan");
  }

  protected void assertRoundRobinQueryPlans() {
    for (int i = 0; i < 3; i++) {
      assertThat(policy.newQueryPlan(request, session))
          .containsExactly(node1, node2, node3, node4, node5);
      assertThat(policy.newQueryPlan(request, session))
          .containsExactly(node2, node3, node4, node5, node1);
      assertThat(policy.newQueryPlan(request, session))
          .containsExactly(node3, node4, node5, node1, node2);
      assertThat(policy.newQueryPlan(request, session))
          .containsExactly(node4, node5, node1, node2, node3);
      assertThat(policy.newQueryPlan(request, session))
          .containsExactly(node5, node1, node2, node3, node4);
    }
  }

  @Test
  public void should_prioritize_single_replica() {
    when(request.getRoutingKeyspace()).thenReturn(KEYSPACE);
    when(request.getRoutingKey()).thenReturn(ROUTING_KEY);
    when(tokenMap.getReplicas(KEYSPACE, null, ROUTING_KEY)).thenReturn(ImmutableSet.of(node3));

    // node3 always first, round-robin on the rest
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node1, node2, node4, node5);
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node2, node4, node5, node1);
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node4, node5, node1, node2);
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node5, node1, node2, node4);

    // Should not shuffle replicas since there is only one
    verify(policy, never()).shuffleHead(any(), anyInt());
  }

  @Test
  public void should_prioritize_and_shuffle_replicas() {
    when(request.getRoutingKeyspace()).thenReturn(KEYSPACE);
    when(request.getRoutingKey()).thenReturn(ROUTING_KEY);
    when(tokenMap.getReplicas(KEYSPACE, null, ROUTING_KEY))
        .thenReturn(ImmutableSet.of(node3, node5));

    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node5, node1, node2, node4);
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node5, node2, node4, node1);
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node5, node4, node1, node2);

    verify(policy, times(3)).shuffleHead(any(), eq(2));
    // No power of two choices with only two replicas
    verify(session, never()).getPools();
  }

  protected BasicLoadBalancingPolicy createAndInitPolicy() {
    // Use a subclass to disable shuffling, we just spy to make sure that the shuffling method was
    // called (makes tests easier)
    BasicLoadBalancingPolicy policy =
        spy(
            new BasicLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME) {
              @Override
              protected void shuffleHead(Object[] currentNodes, int headLength) {
                // nothing (keep in same order)
              }
            });
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1,
            UUID.randomUUID(), node2,
            UUID.randomUUID(), node3,
            UUID.randomUUID(), node4,
            UUID.randomUUID(), node5),
        distanceReporter);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2, node3, node4, node5);
    return policy;
  }
}
