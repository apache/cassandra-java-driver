/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.session.CqlSession;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.isBootstrapping;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;

public class SpeculativeExecutionIT {

  // Note: it looks like shorter delays cause precision issues with Netty timers
  private static final long SPECULATIVE_DELAY = 100;

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

    try (Cluster<CqlSession> cluster = buildCluster(2, SPECULATIVE_DELAY)) {
      CqlSession session = cluster.connect();
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

    try (Cluster<CqlSession> cluster = buildCluster(2, SPECULATIVE_DELAY)) {
      CqlSession session = cluster.connect();
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

    try (Cluster<CqlSession> cluster = buildCluster(2, SPECULATIVE_DELAY)) {
      CqlSession session = cluster.connect();
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

    try (Cluster<CqlSession> cluster = buildCluster(3, SPECULATIVE_DELAY)) {
      CqlSession session = cluster.connect();
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

    try (Cluster<CqlSession> cluster = buildCluster(3, SPECULATIVE_DELAY)) {
      CqlSession session = cluster.connect();
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

    try (Cluster<CqlSession> cluster = buildCluster(3, SPECULATIVE_DELAY)) {
      CqlSession session = cluster.connect();
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

    try (Cluster<CqlSession> cluster = buildCluster(2, SPECULATIVE_DELAY)) {
      CqlSession session = cluster.connect();
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
    try (Cluster<CqlSession> cluster = buildCluster(3, SPECULATIVE_DELAY)) {
      CqlSession session = cluster.connect();
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

    try (Cluster<CqlSession> cluster = buildCluster(3, 0)) {
      CqlSession session = cluster.connect();
      ResultSet resultSet = session.execute(QUERY);

      assertThat(resultSet.getExecutionInfo().getSuccessfulExecutionIndex()).isEqualTo(2);
      assertThat(resultSet.getExecutionInfo().getSpeculativeExecutionCount()).isEqualTo(2);

      assertQueryCount(0, 1);
      assertQueryCount(1, 1);
      assertQueryCount(2, 1);
    }
  }

  // Build a new Cluster instance for each test, because we need different configurations
  private Cluster<CqlSession> buildCluster(int maxSpeculativeExecutions, long speculativeDelayMs) {
    return ClusterUtils.newCluster(
        simulacron,
        String.format("request.timeout = %d milliseconds", SPECULATIVE_DELAY * 10),
        "request.default-idempotence = true",
        "load-balancing-policy.class = com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy",
        "request.speculative-execution-policy.class = com.datastax.oss.driver.api.core.specex.ConstantSpeculativeExecutionPolicy",
        String.format(
            "request.speculative-execution-policy.max-executions = %d", maxSpeculativeExecutions),
        String.format(
            "request.speculative-execution-policy.delay = %d milliseconds", speculativeDelayMs));
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
