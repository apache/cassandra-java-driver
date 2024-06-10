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
package com.datastax.oss.driver.core.specex;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.isBootstrapping;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.QueryCounter;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.specex.ConstantSpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.specex.NoSpeculativeExecutionPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class SpeculativeExecutionIT {

  // Note: it looks like shorter delays cause precision issues with Netty timers
  private static final long SPECULATIVE_DELAY = 1000;

  private static final String QUERY_STRING = "select * from foo";
  private static final SimpleStatement QUERY = SimpleStatement.newInstance(QUERY_STRING);

  // Shared across all tests methods.
  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  private final QueryCounter counter =
      QueryCounter.builder(SIMULACRON_RULE.cluster())
          .withFilter((l) -> l.getQuery().equals(QUERY_STRING))
          .build();

  @Before
  public void clear() {
    SIMULACRON_RULE.cluster().clearPrimes(true);
  }

  @Test
  public void should_not_start_speculative_executions_if_not_idempotent() {
    primeNode(
        0, when(QUERY_STRING).then(noRows()).delay(3 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));

    try (CqlSession session = buildSession(2, SPECULATIVE_DELAY)) {
      ResultSet resultSet = session.execute(QUERY.setIdempotent(false));

      assertThat(resultSet.getExecutionInfo().getSuccessfulExecutionIndex()).isEqualTo(0);
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(0);

      counter.assertNodeCounts(1, 0, 0);
    }
  }

  @Test
  public void should_complete_from_first_speculative_execution_if_faster() {
    primeNode(
        0, when(QUERY_STRING).then(noRows()).delay(3 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    primeNode(1, when(QUERY_STRING).then(noRows()));

    try (CqlSession session = buildSession(2, SPECULATIVE_DELAY)) {
      ResultSet resultSet = session.execute(QUERY);

      assertThat(resultSet.getExecutionInfo().getSuccessfulExecutionIndex()).isEqualTo(1);
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(1);

      counter.assertNodeCounts(1, 1, 0);
    }
  }

  @Test
  public void should_complete_from_initial_execution_if_speculative_is_started_but_slower() {
    primeNode(
        0, when(QUERY_STRING).then(noRows()).delay(3 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    primeNode(
        1, when(QUERY_STRING).then(noRows()).delay(3 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));

    try (CqlSession session = buildSession(2, SPECULATIVE_DELAY)) {
      ResultSet resultSet = session.execute(QUERY);

      assertThat(resultSet.getExecutionInfo().getSuccessfulExecutionIndex()).isEqualTo(0);
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(1);

      counter.assertNodeCounts(1, 1, 0);
    }
  }

  @Test
  public void should_complete_from_second_speculative_execution_if_faster() {
    primeNode(
        0, when(QUERY_STRING).then(noRows()).delay(3 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    primeNode(
        1, when(QUERY_STRING).then(noRows()).delay(3 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    primeNode(2, when(QUERY_STRING).then(noRows()));

    try (CqlSession session = buildSession(3, SPECULATIVE_DELAY)) {
      ResultSet resultSet = session.execute(QUERY);

      assertThat(resultSet.getExecutionInfo().getSuccessfulExecutionIndex()).isEqualTo(2);
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(2);

      counter.assertNodeCounts(1, 1, 1);
    }
  }

  @Test
  public void should_retry_within_initial_execution() {
    // This triggers a retry on the next node:
    primeNode(0, when(QUERY_STRING).then(isBootstrapping()));
    primeNode(1, when(QUERY_STRING).then(noRows()));

    try (CqlSession session = buildSession(3, SPECULATIVE_DELAY)) {
      ResultSet resultSet = session.execute(QUERY);

      assertThat(resultSet.getExecutionInfo().getSuccessfulExecutionIndex()).isEqualTo(0);
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(0);

      counter.assertNodeCounts(1, 1, 0);
    }
  }

  @Test
  public void should_retry_within_speculative_execution() {
    primeNode(
        0, when(QUERY_STRING).then(noRows()).delay(3 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    // This triggers a retry on the next node:
    primeNode(1, when(QUERY_STRING).then(isBootstrapping()));
    primeNode(2, when(QUERY_STRING).then(noRows()));

    try (CqlSession session = buildSession(3, SPECULATIVE_DELAY)) {
      ResultSet resultSet = session.execute(QUERY);

      assertThat(resultSet.getExecutionInfo().getSuccessfulExecutionIndex()).isEqualTo(1);
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(1);

      counter.assertNodeCounts(1, 1, 1);
    }
  }

  @Test
  public void should_wait_for_last_execution_to_complete() {
    // Initial execution uses node0 which takes a long time to reply
    primeNode(
        0, when(QUERY_STRING).then(noRows()).delay(3 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    // specex1 runs fast, but only encounters failing nodes and stops
    primeNode(1, when(QUERY_STRING).then(isBootstrapping()));
    primeNode(2, when(QUERY_STRING).then(isBootstrapping()));

    try (CqlSession session = buildSession(2, SPECULATIVE_DELAY)) {
      ResultSet resultSet = session.execute(QUERY);

      assertThat(resultSet.getExecutionInfo().getSuccessfulExecutionIndex()).isEqualTo(0);
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(1);

      counter.assertNodeCounts(1, 1, 1);
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_fail_if_all_executions_reach_end_of_query_plan() {
    // Each execution gets a BOOTSTRAPPING response, but by the time it retries, the query plan will
    // be empty.
    for (int i = 0; i < 3; i++) {
      primeNode(
          i,
          when(QUERY_STRING)
              .then(isBootstrapping())
              .delay((3 - i) * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    }
    try (CqlSession session = buildSession(3, SPECULATIVE_DELAY)) {
      session.execute(QUERY);
    } finally {
      counter.assertNodeCounts(1, 1, 1);
    }
  }

  @Test
  public void should_allow_zero_delay() {
    // All executions start at the same time, but one of them is faster
    for (int i = 0; i < 2; i++) {
      primeNode(
          i, when(QUERY_STRING).then(noRows()).delay(2 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    }
    primeNode(2, when(QUERY_STRING).then(noRows()).delay(SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));

    try (CqlSession session = buildSession(3, 0)) {
      ResultSet resultSet = session.execute(QUERY);

      assertThat(resultSet.getExecutionInfo().getSuccessfulExecutionIndex()).isEqualTo(2);
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(2);

      counter.assertNodeCounts(1, 1, 1);
    }
  }

  @Test
  public void should_use_policy_from_request_profile() {
    // each host takes same amount of time
    for (int i = 0; i < 2; i++) {
      primeNode(
          i, when(QUERY_STRING).then(noRows()).delay(2 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    }

    // Set large delay for default so we ensure profile is used.
    try (CqlSession session = buildSessionWithProfile(3, 100, 2, 0)) {
      ResultSet resultSet = session.execute(QUERY.setExecutionProfileName("profile1"));

      // Expect only 1 speculative execution as that is all profile called for.
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(1);

      // Expect node 0 and 1 to be queried, but not 2.
      counter.assertNodeCounts(1, 1, 0);
    }
  }

  @Test
  public void should_use_policy_from_request_profile_when_not_configured_in_config() {
    // each host takes same amount of time
    for (int i = 0; i < 2; i++) {
      primeNode(
          i, when(QUERY_STRING).then(noRows()).delay(2 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    }

    // Disable in primary configuration
    try (CqlSession session = buildSessionWithProfile(-1, -1, 3, 0)) {
      ResultSet resultSet = session.execute(QUERY.setExecutionProfileName("profile1"));

      // Expect speculative executions on each node.
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(2);

      // Expect all nodes to be queried.
      counter.assertNodeCounts(1, 1, 1);
    }
  }

  @Test
  public void should_use_policy_from_config_when_not_configured_in_request_profile() {
    // each host takes same amount of time
    for (int i = 0; i < 2; i++) {
      primeNode(
          i, when(QUERY_STRING).then(noRows()).delay(2 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    }

    try (CqlSession session = buildSessionWithProfile(3, 0, 3, 0)) {
      // use profile where speculative execution is not configured.
      ResultSet resultSet = session.execute(QUERY.setExecutionProfileName("profile2"));

      // Expect speculative executions on each node since default configuration is used.
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(2);

      counter.assertNodeCounts(1, 1, 1);
    }
  }

  @Test
  public void should_not_speculatively_execute_when_defined_in_profile() {
    // each host takes same amount of time
    for (int i = 0; i < 2; i++) {
      primeNode(
          i, when(QUERY_STRING).then(noRows()).delay(2 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));
    }

    // Disable in profile
    try (CqlSession session = buildSessionWithProfile(3, 100, -1, -1)) {
      ResultSet resultSet = session.execute(QUERY.setExecutionProfileName("profile1"));

      // Expect no speculative executions.
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(0);

      // Expect only node 0 to be queried since speculative execution is disabled for this profile.
      counter.assertNodeCounts(1, 0, 0);
    }
  }

  // Build a new Cluster instance for each test, because we need different configurations
  private CqlSession buildSession(int maxSpeculativeExecutions, long speculativeDelayMs) {
    return SessionUtils.newSession(
        SIMULACRON_RULE,
        SessionUtils.configLoaderBuilder()
            .withDuration(
                DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(SPECULATIVE_DELAY * 10))
            .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
            .withClass(
                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, SortingLoadBalancingPolicy.class)
            .withClass(
                DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
                ConstantSpeculativeExecutionPolicy.class)
            .withInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, maxSpeculativeExecutions)
            .withDuration(
                DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY,
                Duration.ofMillis(speculativeDelayMs))
            .build());
  }

  private CqlSession buildSessionWithProfile(
      int defaultMaxSpeculativeExecutions,
      long defaultSpeculativeDelayMs,
      int profile1MaxSpeculativeExecutions,
      long profile1SpeculativeDelayMs) {

    ProgrammaticDriverConfigLoaderBuilder builder =
        SessionUtils.configLoaderBuilder()
            .withDuration(
                DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(SPECULATIVE_DELAY * 10))
            .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
            .withClass(
                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, SortingLoadBalancingPolicy.class);

    if (defaultMaxSpeculativeExecutions != -1 || defaultSpeculativeDelayMs != -1) {
      builder =
          builder.withClass(
              DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
              ConstantSpeculativeExecutionPolicy.class);
      if (defaultMaxSpeculativeExecutions != -1) {
        builder =
            builder.withInt(
                DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, defaultMaxSpeculativeExecutions);
      }
      if (defaultSpeculativeDelayMs != -1) {
        builder =
            builder.withDuration(
                DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY,
                Duration.ofMillis(defaultSpeculativeDelayMs));
      }
    } else {
      builder =
          builder.withClass(
              DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
              NoSpeculativeExecutionPolicy.class);
    }

    builder = builder.startProfile("profile1");
    if (profile1MaxSpeculativeExecutions != -1 || profile1SpeculativeDelayMs != -1) {
      builder =
          builder.withClass(
              DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
              ConstantSpeculativeExecutionPolicy.class);

      if (profile1MaxSpeculativeExecutions != -1) {
        builder =
            builder.withInt(
                DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, profile1MaxSpeculativeExecutions);
      }
      if (profile1SpeculativeDelayMs != -1) {
        builder =
            builder.withDuration(
                DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY,
                Duration.ofMillis(profile1SpeculativeDelayMs));
      }
    } else {
      builder =
          builder.withClass(
              DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
              NoSpeculativeExecutionPolicy.class);
    }

    builder =
        builder.startProfile("profile2").withString(DefaultDriverOption.REQUEST_CONSISTENCY, "ONE");

    CqlSession session = SessionUtils.newSession(SIMULACRON_RULE, builder.build());

    // validate profile data
    DriverContext context = session.getContext();
    DriverConfig driverConfig = context.getConfig();
    assertThat(driverConfig.getProfiles()).containsKeys("profile1", "profile2");

    assertThat(context.getSpeculativeExecutionPolicies())
        .hasSize(3)
        .containsKeys(DriverExecutionProfile.DEFAULT_NAME, "profile1", "profile2");

    SpeculativeExecutionPolicy defaultPolicy =
        context.getSpeculativeExecutionPolicy(DriverExecutionProfile.DEFAULT_NAME);
    SpeculativeExecutionPolicy policy1 = context.getSpeculativeExecutionPolicy("profile1");
    SpeculativeExecutionPolicy policy2 = context.getSpeculativeExecutionPolicy("profile2");
    Class<? extends SpeculativeExecutionPolicy> expectedDefaultPolicyClass =
        defaultMaxSpeculativeExecutions != -1 || defaultSpeculativeDelayMs != -1
            ? ConstantSpeculativeExecutionPolicy.class
            : NoSpeculativeExecutionPolicy.class;
    assertThat(defaultPolicy).isInstanceOf(expectedDefaultPolicyClass).isSameAs(policy2);

    // If configuration was same, same policy instance should be used.
    if (defaultMaxSpeculativeExecutions == profile1MaxSpeculativeExecutions
        && defaultSpeculativeDelayMs == profile1SpeculativeDelayMs) {
      assertThat(defaultPolicy).isSameAs(policy1);
    } else {
      assertThat(defaultPolicy).isNotSameAs(policy1);
    }

    Class<? extends SpeculativeExecutionPolicy> expectedProfile1PolicyClass =
        profile1MaxSpeculativeExecutions != -1 || profile1SpeculativeDelayMs != -1
            ? ConstantSpeculativeExecutionPolicy.class
            : NoSpeculativeExecutionPolicy.class;
    assertThat(policy1).isInstanceOf(expectedProfile1PolicyClass);

    return session;
  }

  private void primeNode(int id, PrimeDsl.PrimeBuilder primeBuilder) {
    SIMULACRON_RULE.cluster().node(id).prime(primeBuilder);
  }
}
