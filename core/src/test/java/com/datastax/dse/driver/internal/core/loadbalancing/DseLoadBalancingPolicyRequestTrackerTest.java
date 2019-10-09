/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.loadbalancing;

import static com.datastax.oss.driver.api.core.config.DriverExecutionProfile.DEFAULT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class DseLoadBalancingPolicyRequestTrackerTest extends DseLoadBalancingPolicyTestBase {

  private DseLoadBalancingPolicy policy;
  private long nextNanoTime;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1));
    policy =
        new DseLoadBalancingPolicy(context, DEFAULT_NAME) {
          @Override
          long nanoTime() {
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
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(node1, value -> assertThat(value.get(0)).isEqualTo(123L))
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
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(
            node1,
            value -> {
              // oldest value first
              assertThat(value.get(0)).isEqualTo(123);
              assertThat(value.get(1)).isEqualTo(456);
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
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(
            node1,
            value -> {
              // values should rotate left (bubble up)
              assertThat(value.get(0)).isEqualTo(456);
              assertThat(value.get(1)).isEqualTo(789);
            })
        .hasEntrySatisfying(node2, value -> assertThat(value.get(0)).isEqualTo(789))
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
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(node1, value -> assertThat(value.get(0)).isEqualTo(123L))
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
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(
            node1,
            value -> {
              // oldest value first
              assertThat(value.get(0)).isEqualTo(123);
              assertThat(value.get(1)).isEqualTo(456);
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
    assertThat(policy.responseTimes)
        .hasEntrySatisfying(
            node1,
            value -> {
              // values should rotate left (bubble up)
              assertThat(value.get(0)).isEqualTo(456);
              assertThat(value.get(1)).isEqualTo(789);
            })
        .hasEntrySatisfying(node2, value -> assertThat(value.get(0)).isEqualTo(789))
        .doesNotContainKey(node3);
    assertThat(policy.isResponseRateInsufficient(node1, nextNanoTime)).isFalse();
    assertThat(policy.isResponseRateInsufficient(node2, nextNanoTime)).isTrue();
    assertThat(policy.isResponseRateInsufficient(node3, nextNanoTime)).isTrue();
  }
}
