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
package com.datastax.oss.driver.api.core.specex;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.isBootstrapping;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.specex.ConstantSpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.specex.NoSpeculativeExecutionPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class SpeculativeExecutionIT {

  // Note: it looks like shorter delays cause precision issues with Netty timers
  private static final long SPECULATIVE_DELAY = 500;

  private static String QUERY_STRING = "select * from foo";
  private static final SimpleStatement QUERY = SimpleStatement.newInstance(QUERY_STRING);

  // Shared across all tests methods.
  public static @ClassRule SimulacronRule simulacron =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  @Before
  public void clear() {
    simulacron.cluster().clearLogs();
    simulacron.cluster().clearPrimes(true);
  }

  @Test
  public void should_not_start_speculative_executions_if_not_idempotent() {
    primeNode(
        0, when(QUERY_STRING).then(noRows()).delay(3 * SPECULATIVE_DELAY, TimeUnit.MILLISECONDS));

    try (CqlSession session = buildSession(2, SPECULATIVE_DELAY)) {
      ResultSet resultSet = session.execute(QUERY.setIdempotent(false));

      assertThat(resultSet.getExecutionInfo().getSuccessfulExecutionIndex()).isEqualTo(0);
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(0);

      assertQueryCount(0, 1);
      assertQueryCount(1, 0);
      assertQueryCount(2, 0);
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

      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 0);
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

      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 0);
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

      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 1);
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

      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 0);
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

      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 1);
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

      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 1);
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
      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 1);
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

      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 1);
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
      ResultSet resultSet = session.execute(QUERY.setConfigProfileName("profile1"));

      // Expect only 1 speculative execution as that is all profile called for.
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(1);

      // Expect node 0 and 1 to be queried, but not 2.
      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 0);
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
      ResultSet resultSet = session.execute(QUERY.setConfigProfileName("profile1"));

      // Expect speculative executions on each node.
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(2);

      // Expect all nodes to be queried.
      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 1);
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
      ResultSet resultSet = session.execute(QUERY.setConfigProfileName("profile2"));

      // Expect speculative executions on each node since default configuration is used.
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(2);

      // Expect all nodes to be queried.
      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 1);
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
      ResultSet resultSet = session.execute(QUERY.setConfigProfileName("profile1"));

      // Expect no speculative executions.
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(0);

      // Expect only node 0 to be queries since speculative execution is disabled for this profile.
      assertQueryCount(0, 1);
      assertQueryCount(1, 0);
      assertQueryCount(2, 0);
    }
  }

  // Build a new Cluster instance for each test, because we need different configurations
  private CqlSession buildSession(int maxSpeculativeExecutions, long speculativeDelayMs) {
    return SessionUtils.newSession(
        simulacron,
        String.format("request.timeout = %d milliseconds", SPECULATIVE_DELAY * 10),
        "request.default-idempotence = true",
        "load-balancing-policy.class = com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy",
        "request.speculative-execution-policy.class = ConstantSpeculativeExecutionPolicy",
        String.format(
            "request.speculative-execution-policy.max-executions = %d", maxSpeculativeExecutions),
        String.format(
            "request.speculative-execution-policy.delay = %d milliseconds", speculativeDelayMs));
  }

  private CqlSession buildSessionWithProfile(
      int defaultMaxSpeculativeExecutions,
      long defaultSpeculativeDelayMs,
      int profile1MaxSpeculativeExecutions,
      long profile1SpeculativeDelayMs) {

    List<String> config = Lists.newArrayList();
    config.add(String.format("request.timeout = %d milliseconds", SPECULATIVE_DELAY * 10));
    config.add("request.default-idempotence = true");
    config.add(
        "load-balancing-policy.class = com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy");

    if (defaultMaxSpeculativeExecutions != -1 || defaultSpeculativeDelayMs != -1) {
      config.add("request.speculative-execution-policy.class = ConstantSpeculativeExecutionPolicy");
      if (defaultMaxSpeculativeExecutions != -1) {
        config.add(
            String.format(
                "request.speculative-execution-policy.max-executions = %d",
                defaultMaxSpeculativeExecutions));
      }
      if (defaultSpeculativeDelayMs != -1) {
        config.add(
            String.format(
                "request.speculative-execution-policy.delay = %d milliseconds",
                defaultSpeculativeDelayMs));
      }
    } else {
      config.add("request.speculative-execution-policy.class = NoSpeculativeExecutionPolicy");
    }

    if (profile1MaxSpeculativeExecutions != -1 || profile1SpeculativeDelayMs != -1) {
      config.add(
          "profiles.profile1.request.speculative-execution-policy.class = ConstantSpeculativeExecutionPolicy");
      if (profile1MaxSpeculativeExecutions != -1) {
        config.add(
            String.format(
                "profiles.profile1.request.speculative-execution-policy.max-executions = %d",
                profile1MaxSpeculativeExecutions));
      }
      if (profile1SpeculativeDelayMs != -1) {
        config.add(
            String.format(
                "profiles.profile1.request.speculative-execution-policy.delay = %d milliseconds",
                profile1SpeculativeDelayMs));
      }
    } else {
      config.add(
          "profiles.profile1.request.speculative-execution-policy.class = NoSpeculativeExecutionPolicy");
    }

    config.add("profiles.profile2 = {}");

    CqlSession session = SessionUtils.newSession(simulacron, config.toArray(new String[0]));

    // validate profile data
    DriverContext context = session.getContext();
    DriverConfig driverConfig = context.config();
    assertThat(driverConfig.getNamedProfiles()).containsKeys("profile1", "profile2");

    assertThat(context.speculativeExecutionPolicies())
        .hasSize(3)
        .containsKeys(DriverConfigProfile.DEFAULT_NAME, "profile1", "profile2");

    SpeculativeExecutionPolicy defaultPolicy =
        context.speculativeExecutionPolicy(DriverConfigProfile.DEFAULT_NAME);
    SpeculativeExecutionPolicy policy1 = context.speculativeExecutionPolicy("profile1");
    SpeculativeExecutionPolicy policy2 = context.speculativeExecutionPolicy("profile2");
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
    simulacron.cluster().node(id).prime(primeBuilder);
  }

  private void assertQueryCount(int node, int expected) {
    assertThat(
            simulacron
                .cluster()
                .node(node)
                .getLogs()
                .getQueryLogs()
                .stream()
                .filter(l -> l.getQuery().equals(QUERY_STRING)))
        .as("Expected query count to be %d for node %d", expected, node)
        .hasSize(expected);
  }
}
