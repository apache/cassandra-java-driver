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
package com.datastax.oss.driver.internal.core.loadbalancing;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.api.core.config.DriverExecutionProfile.DEFAULT_NAME;
import static org.mockito.BDDMockito.given;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class DefaultLoadBalancingPolicyRequestTrackerTest extends LoadBalancingPolicyTestBase {

  @Mock Request request;
  @Mock DriverExecutionProfile profile;
  final String logPrefix = "lbp-test-log-prefix";

  private DefaultLoadBalancingPolicy policy;
  private long nextNanoTime;

  @Before
  @Override
  public void setup() {
    super.setup();
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1));
    policy =
        new DefaultLoadBalancingPolicy(context, DEFAULT_NAME) {
          @Override
          protected long nanoTime() {
            return nextNanoTime;
          }
        };
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1,
            UUID.randomUUID(), node2,
            UUID.randomUUID(), node3),
        distanceReporter);
  }

  @Test
  public void should_record_first_response_time_on_node_success() {
    // Given
    nextNanoTime = 123;

    // When
    policy.onNodeSuccess(request, 0, profile, node1, logPrefix);

    // Then
    assertThat(policy.responseTimes.asMap())
        .hasEntrySatisfying(node1, value -> assertThat(value.times.get(0)).isEqualTo(123L))
        .doesNotContainKeys(node2, node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }

  @Test
  public void should_record_second_response_time_on_node_success() {
    // Given
    should_record_first_response_time_on_node_success();
    nextNanoTime = 456;

    // When
    policy.onNodeSuccess(request, 0, profile, node1, logPrefix);

    // Then
    assertThat(policy.responseTimes.asMap())
        .hasEntrySatisfying(
            node1,
            value -> {
              // oldest value first
              assertThat(value.times.get(0)).isEqualTo(123);
              assertThat(value.times.get(1)).isEqualTo(456);
            })
        .doesNotContainKeys(node2, node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isFalse();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }

  @Test
  public void should_record_further_response_times_on_node_success() {
    // Given
    should_record_second_response_time_on_node_success();
    nextNanoTime = 789;

    // When
    policy.onNodeSuccess(request, 0, profile, node1, logPrefix);
    policy.onNodeSuccess(request, 0, profile, node2, logPrefix);

    // Then
    assertThat(policy.responseTimes.asMap())
        .hasEntrySatisfying(
            node1,
            value -> {
              // values should rotate left (bubble up)
              assertThat(value.times.get(0)).isEqualTo(456);
              assertThat(value.times.get(1)).isEqualTo(789);
            })
        .hasEntrySatisfying(node2, value -> assertThat(value.times.get(0)).isEqualTo(789))
        .doesNotContainKey(node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isFalse();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }

  @Test
  public void should_record_first_response_time_on_node_error() {
    // Given
    nextNanoTime = 123;
    Throwable iae = new IllegalArgumentException();

    // When
    policy.onNodeError(request, iae, 0, profile, node1, logPrefix);

    // Then
    assertThat(policy.responseTimes.asMap())
        .hasEntrySatisfying(node1, value -> assertThat(value.times.get(0)).isEqualTo(123L))
        .doesNotContainKeys(node2, node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }

  @Test
  public void should_record_second_response_time_on_node_error() {
    // Given
    should_record_first_response_time_on_node_error();
    nextNanoTime = 456;
    Throwable iae = new IllegalArgumentException();

    // When
    policy.onNodeError(request, iae, 0, profile, node1, logPrefix);

    // Then
    assertThat(policy.responseTimes.asMap())
        .hasEntrySatisfying(
            node1,
            value -> {
              // oldest value first
              assertThat(value.times.get(0)).isEqualTo(123);
              assertThat(value.times.get(1)).isEqualTo(456);
            })
        .doesNotContainKeys(node2, node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isFalse();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }

  @Test
  public void should_record_further_response_times_on_node_error() {
    // Given
    should_record_second_response_time_on_node_error();
    nextNanoTime = 789;
    Throwable iae = new IllegalArgumentException();

    // When
    policy.onNodeError(request, iae, 0, profile, node1, logPrefix);
    policy.onNodeError(request, iae, 0, profile, node2, logPrefix);

    // Then
    assertThat(policy.responseTimes.asMap())
        .hasEntrySatisfying(
            node1,
            value -> {
              // values should rotate left (bubble up)
              assertThat(value.times.get(0)).isEqualTo(456);
              assertThat(value.times.get(1)).isEqualTo(789);
            })
        .hasEntrySatisfying(node2, value -> assertThat(value.times.get(0)).isEqualTo(789))
        .doesNotContainKey(node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isFalse();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }
}
