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
package com.datastax.oss.driver.core.cql.reactive;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.codahale.metrics.Metric;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.loadbalancing.NodeComparator;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.core.retry.PerProfileRetryPolicyIT.NoRetryPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.google.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.reactivex.Flowable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeSet;
import java.util.UUID;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/** Small test to validate the application-level retry behavior explained in the manual. */
@Category(ParallelizableTests.class)
public class ReactiveRetryIT {

  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
                  .withClass(
                      DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                      CyclingLoadBalancingPolicy.class)
                  .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, NoRetryPolicy.class)
                  .withStringList(
                      DefaultDriverOption.METRICS_NODE_ENABLED,
                      Collections.singletonList("errors.request.unavailables"))
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  private static final String QUERY_STRING = "select * from foo";

  private List<Node> nodes;

  @Before
  public void clearPrimes() {
    SIMULACRON_RULE.cluster().clearLogs();
    SIMULACRON_RULE.cluster().clearPrimes(true);
  }

  @Before
  public void createNodesList() {
    nodes = new ArrayList<>(SESSION_RULE.session().getMetadata().getNodes().values());
    nodes.sort(NodeComparator.INSTANCE);
  }

  @Test
  public void should_retry_at_application_level() {
    // Given
    CqlSession session = spy(SESSION_RULE.session());
    BoundCluster cluster = SIMULACRON_RULE.cluster();
    cluster.node(0).prime(when(QUERY_STRING).then(unavailable(ConsistencyLevel.ONE, 1, 0)));
    cluster.node(1).prime(when(QUERY_STRING).then(unavailable(ConsistencyLevel.ONE, 1, 0)));
    cluster.node(2).prime(when(QUERY_STRING).then(rows().row("col1", "Yay!")));

    // When
    ReactiveRow row =
        Flowable.defer(() -> session.executeReactive(QUERY_STRING))
            .retry(
                (retry, error) -> {
                  assertThat(error).isInstanceOf(UnavailableException.class);
                  UnavailableException ue = (UnavailableException) error;
                  Node coordinator = ue.getCoordinator();
                  if (retry == 1) {
                    assertCoordinator(0, coordinator);
                    return true;
                  } else if (retry == 2) {
                    assertCoordinator(1, coordinator);
                    return true;
                  } else {
                    fail("Unexpected retry attempt");
                    return false;
                  }
                })
            .blockingLast();

    // Then
    assertThat(row.getString(0)).isEqualTo("Yay!");
    verify(session, times(3)).executeReactive(QUERY_STRING);
    assertUnavailableMetric(0, 1L);
    assertUnavailableMetric(1, 1L);
    assertUnavailableMetric(2, 0L);
  }

  private void assertCoordinator(int expectedNodeIndex, Node actual) {
    Node expected = nodes.get(expectedNodeIndex);
    assertThat(actual).isSameAs(expected);
  }

  private void assertUnavailableMetric(int nodeIndex, long expectedUnavailableCount) {
    Metrics metrics = SESSION_RULE.session().getMetrics().orElseThrow(AssertionError::new);
    Node node = nodes.get(nodeIndex);
    Optional<Metric> expectedMetric = metrics.getNodeMetric(node, DefaultNodeMetric.UNAVAILABLES);
    assertThat(expectedMetric)
        .isPresent()
        .hasValueSatisfying(
            metric -> assertThat(metric).extracting("count").isEqualTo(expectedUnavailableCount));
  }

  public static class CyclingLoadBalancingPolicy implements LoadBalancingPolicy {

    private final TreeSet<Node> nodes = new TreeSet<>(NodeComparator.INSTANCE);
    private volatile Iterator<Node> iterator = Iterables.cycle(nodes).iterator();

    @SuppressWarnings("unused")
    public CyclingLoadBalancingPolicy(DriverContext context, String profileName) {
      // constructor needed for loading via config.
    }

    @Override
    public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
      this.nodes.addAll(nodes.values());
      this.nodes.forEach(n -> distanceReporter.setDistance(n, NodeDistance.LOCAL));
      iterator = Iterables.cycle(this.nodes).iterator();
    }

    @NonNull
    @Override
    public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
      return new ArrayDeque<>(Collections.singleton(iterator.next()));
    }

    @Override
    public void onAdd(@NonNull Node node) {}

    @Override
    public void onUp(@NonNull Node node) {}

    @Override
    public void onDown(@NonNull Node node) {}

    @Override
    public void onRemove(@NonNull Node node) {}

    @Override
    public void close() {}
  }
}
