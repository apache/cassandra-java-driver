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
package com.datastax.oss.driver.core.metrics;

import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_QUORUM;
import static com.datastax.oss.simulacron.common.codec.WriteType.BATCH;
import static com.datastax.oss.simulacron.common.stubbing.CloseType.DISCONNECT;
import static com.datastax.oss.simulacron.common.stubbing.DisconnectAction.Scope.CONNECTION;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.closeConnection;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.readTimeout;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.writeTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.BasicLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metrics.AbstractMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.DefaultMetricIdGenerator;
import com.datastax.oss.driver.internal.core.metrics.TaggingMetricIdGenerator;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.driver.internal.core.specex.ConstantSpeculativeExecutionPolicy;
import com.datastax.oss.driver.shaded.guava.common.collect.Queues;
import com.datastax.oss.simulacron.server.BoundNode;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public abstract class MetricsITBase {

  protected static final List<DefaultSessionMetric> ENABLED_SESSION_METRICS =
      Arrays.asList(DefaultSessionMetric.values());

  protected static final List<DefaultNodeMetric> ENABLED_NODE_METRICS =
      Arrays.asList(DefaultNodeMetric.values());

  protected abstract SimulacronRule simulacron();

  protected abstract Object newMetricRegistry();

  protected abstract String getMetricsFactoryClass();

  protected abstract void assertMetricsPresent(CqlSession session);

  protected abstract void assertNodeMetricsEvicted(CqlSession session, Node node) throws Exception;

  protected abstract void assertNodeMetricsNotEvicted(CqlSession session, Node node)
      throws Exception;

  @Before
  public void clearPrimes() {
    simulacron().cluster().clearLogs();
    simulacron().cluster().clearPrimes(true);
  }

  @Test
  @UseDataProvider("descriptorsAndPrefixes")
  public void should_expose_metrics_if_enabled(Class<?> metricIdGenerator, String prefix) {

    DriverConfigLoader loader =
        allMetricsEnabled()
            .withString(
                DefaultDriverOption.METRICS_ID_GENERATOR_CLASS, metricIdGenerator.getSimpleName())
            .withString(DefaultDriverOption.METRICS_ID_GENERATOR_PREFIX, prefix)
            .withString(
                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                DeterministicLoadBalancingPolicy.class.getName())
            .withString(
                DefaultDriverOption.RETRY_POLICY_CLASS, DeterministicRetryPolicy.class.getName())
            .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
            .withString(
                DefaultDriverOption.RECONNECTION_POLICY_CLASS,
                ConstantReconnectionPolicy.class.getName())
            .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(1))
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(2))
            .startProfile("spec_exec")
            .withString(
                DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
                ConstantSpeculativeExecutionPolicy.class.getName())
            .withInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, 2)
            .withDuration(DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY, Duration.ZERO)
            .endProfile()
            .build();

    DeterministicRetryPolicy.verdict = null;
    DeterministicLoadBalancingPolicy.queryPlan = null;

    try (CqlSession session =
        CqlSession.builder()
            .addContactEndPoints(simulacron().getContactPoints())
            .withConfigLoader(loader)
            .withMetricRegistry(newMetricRegistry())
            .build()) {

      awaitClusterUp(session);
      session.prepare("irrelevant");
      queryAllNodes(session);
      triggerErrors(session, RetryVerdict.RETRY_NEXT);
      triggerErrors(session, RetryVerdict.IGNORE);
      triggerErrors(session, RetryVerdict.RETHROW);
      triggerTimeouts(session);
      awaitClusterUp(session);

      assertMetricsPresent(session);

    } catch (Exception e) {

      e.printStackTrace();

      // Dropwizard is incompatible with metric tags
      assertThat(metricIdGenerator.getSimpleName()).contains("Tagging");
      assertThat(getMetricsFactoryClass()).contains("Dropwizard");
      assertThat(e)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot use metric tags with Dropwizard");
    }
  }

  @DataProvider
  public static Object[][] descriptorsAndPrefixes() {
    return new Object[][] {
      new Object[] {DefaultMetricIdGenerator.class, ""},
      new Object[] {DefaultMetricIdGenerator.class, "cassandra"},
      new Object[] {TaggingMetricIdGenerator.class, ""},
      new Object[] {TaggingMetricIdGenerator.class, "cassandra"},
    };
  }

  @Test
  public void should_not_expose_metrics_if_disabled() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, Collections.emptyList())
            .withStringList(DefaultDriverOption.METRICS_NODE_ENABLED, Collections.emptyList())
            .withString(DefaultDriverOption.METRICS_FACTORY_CLASS, getMetricsFactoryClass())
            .build();
    try (CqlSession session =
        CqlSession.builder()
            .addContactEndPoints(simulacron().getContactPoints())
            .withConfigLoader(loader)
            .build()) {
      queryAllNodes(session);
      MetricRegistry registry =
          (MetricRegistry) ((InternalDriverContext) session.getContext()).getMetricRegistry();
      assertThat(registry).isNull();
      assertThat(session.getMetrics()).isEmpty();
    }
  }

  @Test
  public void should_evict_down_node_metrics_when_timeout_fires() throws Exception {
    // given
    Duration expireAfter = Duration.ofSeconds(1);
    DriverConfigLoader loader =
        allMetricsEnabled()
            .withDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER, expireAfter)
            .build();

    AbstractMetricUpdater.MIN_EXPIRE_AFTER = expireAfter;

    try (CqlSession session =
        CqlSession.builder()
            .addContactEndPoints(simulacron().getContactPoints())
            .withConfigLoader(loader)
            .withMetricRegistry(newMetricRegistry())
            .build()) {

      queryAllNodes(session);

      DefaultNode node1 = findNode(session, 0);
      DefaultNode node2 = findNode(session, 1);
      DefaultNode node3 = findNode(session, 2);

      EventBus eventBus = ((InternalDriverContext) session.getContext()).getEventBus();

      // trigger node1 UP -> DOWN
      eventBus.fire(NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, node1));

      Thread.sleep(expireAfter.toMillis());

      // then node-level metrics should be evicted from node1, but
      // node2 and node3 metrics should not have been evicted
      await().untilAsserted(() -> assertNodeMetricsEvicted(session, node1));
      assertNodeMetricsNotEvicted(session, node2);
      assertNodeMetricsNotEvicted(session, node3);

    } finally {
      AbstractMetricUpdater.MIN_EXPIRE_AFTER = Duration.ofMinutes(5);
    }
  }

  @Test
  public void should_not_evict_down_node_metrics_when_node_is_back_up_before_timeout()
      throws Exception {
    // given
    Duration expireAfter = Duration.ofSeconds(2);
    DriverConfigLoader loader =
        allMetricsEnabled()
            .withDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER, expireAfter)
            .build();

    AbstractMetricUpdater.MIN_EXPIRE_AFTER = expireAfter;

    try (CqlSession session =
        CqlSession.builder()
            .addContactEndPoints(simulacron().getContactPoints())
            .withConfigLoader(loader)
            .withMetricRegistry(newMetricRegistry())
            .build()) {

      queryAllNodes(session);

      DefaultNode node1 = findNode(session, 0);
      DefaultNode node2 = findNode(session, 1);
      DefaultNode node3 = findNode(session, 2);

      EventBus eventBus = ((InternalDriverContext) session.getContext()).getEventBus();

      // trigger nodes UP -> DOWN
      eventBus.fire(NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, node1));
      eventBus.fire(NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, node2));
      eventBus.fire(NodeStateEvent.removed(node3));

      Thread.sleep(500);

      // trigger nodes DOWN -> UP, should cancel the timeouts
      eventBus.fire(NodeStateEvent.changed(NodeState.DOWN, NodeState.UP, node1));
      eventBus.fire(NodeStateEvent.changed(NodeState.FORCED_DOWN, NodeState.UP, node2));
      eventBus.fire(NodeStateEvent.added(node3));

      Thread.sleep(expireAfter.toMillis());

      // then no node-level metrics should be evicted
      assertNodeMetricsNotEvicted(session, node1);
      assertNodeMetricsNotEvicted(session, node2);
      assertNodeMetricsNotEvicted(session, node3);

    } finally {
      AbstractMetricUpdater.MIN_EXPIRE_AFTER = Duration.ofMinutes(5);
    }
  }

  private ProgrammaticDriverConfigLoaderBuilder allMetricsEnabled() {
    return SessionUtils.configLoaderBuilder()
        .withStringList(
            DefaultDriverOption.METRICS_SESSION_ENABLED,
            ENABLED_SESSION_METRICS.stream()
                .map(DefaultSessionMetric::getPath)
                .collect(Collectors.toList()))
        .withStringList(
            DefaultDriverOption.METRICS_NODE_ENABLED,
            ENABLED_NODE_METRICS.stream()
                .map(DefaultNodeMetric::getPath)
                .collect(Collectors.toList()))
        .withString(DefaultDriverOption.METRICS_FACTORY_CLASS, getMetricsFactoryClass());
  }

  private void queryAllNodes(CqlSession session) {
    for (Node node : session.getMetadata().getNodes().values()) {
      for (int i = 0; i < 10; i++) {
        session.execute(SimpleStatement.newInstance("irrelevant").setNode(node));
      }
    }
  }

  private DefaultNode findNode(CqlSession session, int id) {
    InetSocketAddress address1 = simulacron().cluster().node(id).inetSocketAddress();
    return (DefaultNode)
        session.getMetadata().findNode(address1).orElseThrow(IllegalStateException::new);
  }

  // RETRY NEXT: 4*2 + 1 =  9 messages/node, 5*3*1 = 15 requests/session
  // IGNORE    : 4*1 + 0 =  4 messages/node, 5*3*1 = 15 requests/session
  // RETHROW   : 4*1 + 0 =  4 messages/node, 5*3*1 = 15 requests/session
  // TOTAL     :           17 messages/node          30 requests/session
  private void triggerErrors(CqlSession session, RetryVerdict verdict) {

    DeterministicRetryPolicy.verdict = verdict;

    for (int i = 0; i < 3; i++) {

      BoundNode node = simulacron().cluster().node(i);
      DefaultNode first = findNode(session, i % 3);
      DefaultNode second = findNode(session, (i + 1) % 3);
      DeterministicLoadBalancingPolicy.setQueryPlan(first, second);

      try {
        node.prime(when("irrelevant").then(readTimeout(LOCAL_QUORUM, 2, 1, false)));
        session.execute("irrelevant");
        assertThat(verdict).isNotEqualTo(RetryVerdict.RETHROW);
      } catch (ReadTimeoutException e) {
        assertThat(verdict).isEqualTo(RetryVerdict.RETHROW);
      } finally {
        node.clearPrimes(true);
      }

      try {
        node.prime(when("irrelevant").then(writeTimeout(LOCAL_QUORUM, 2, 1, BATCH)));
        session.execute("irrelevant");
        assertThat(verdict).isNotEqualTo(RetryVerdict.RETHROW);
      } catch (WriteTimeoutException e) {
        assertThat(verdict).isEqualTo(RetryVerdict.RETHROW);
      } finally {
        node.clearPrimes(true);
      }

      try {
        node.prime(when("irrelevant").then(unavailable(LOCAL_QUORUM, 2, 1)));
        session.execute("irrelevant");
        assertThat(verdict).isNotEqualTo(RetryVerdict.RETHROW);
      } catch (UnavailableException e) {
        assertThat(verdict).isEqualTo(RetryVerdict.RETHROW);
      } finally {
        node.clearPrimes(true);
      }

      try {
        node.prime(when("irrelevant").then(serverError("irrelevant")));
        session.execute("irrelevant");
        assertThat(verdict).isNotEqualTo(RetryVerdict.RETHROW);
      } catch (ServerError e) {
        assertThat(verdict).isEqualTo(RetryVerdict.RETHROW);
      } finally {
        node.clearPrimes(true);
      }

      try {
        node.prime(when("irrelevant").then(closeConnection(CONNECTION, DISCONNECT)));
        session.execute("irrelevant");
        assertThat(verdict).isNotEqualTo(RetryVerdict.RETHROW);
      } catch (ClosedConnectionException e) {
        assertThat(verdict).isEqualTo(RetryVerdict.RETHROW);
      } finally {
        node.clearPrimes(true);
      }
      await().untilAsserted(() -> assertThat(first.getState() == NodeState.UP).isTrue());
    }
  }

  private void awaitClusterUp(CqlSession session) {
    await()
        .untilAsserted(
            () ->
                assertThat(
                        session.getMetadata().getNodes().values().stream()
                            .filter(n -> n.getState() == NodeState.UP)
                            .count())
                    .isEqualTo(3));
  }

  // timeouts do not increment cql-messages and cql-requests, but the speculative executions will
  // trigger 3 cql-messages (1 per node) and 3 cql-requests.
  private void triggerTimeouts(CqlSession session) {

    DeterministicRetryPolicy.verdict = RetryVerdict.RETHROW;
    SimpleStatement stmt = SimpleStatement.newInstance("irrelevant");

    for (int i = 0; i < 3; i++) {

      BoundNode node = simulacron().cluster().node(i);
      DefaultNode first = findNode(session, i % 3);
      DefaultNode second = findNode(session, (i + 1) % 3);
      DeterministicLoadBalancingPolicy.setQueryPlan(first, second);

      try {
        node.pauseRead();
        try {
          session.execute(stmt);
          fail();
        } catch (DriverTimeoutException ignored) {
        }
        session.execute(stmt.setExecutionProfileName("spec_exec"));
      } finally {
        node.resumeRead();
      }
    }
  }

  public static class DeterministicLoadBalancingPolicy extends BasicLoadBalancingPolicy {

    private static volatile Queue<Node> queryPlan;

    static void setQueryPlan(Node... nodes) {
      queryPlan = new ArrayDeque<>(Arrays.asList(nodes));
    }

    public DeterministicLoadBalancingPolicy(
        @NonNull DriverContext context, @NonNull String profileName) {
      super(context, profileName);
    }

    @NonNull
    @Override
    public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
      Queue<Node> plan = queryPlan;
      if (plan == null) {
        plan = super.newQueryPlan(request, session);
      }
      return Queues.newConcurrentLinkedQueue(plan);
    }
  }

  public static class DeterministicRetryPolicy extends DefaultRetryPolicy {

    static volatile RetryVerdict verdict;

    public DeterministicRetryPolicy(DriverContext context, String profileName) {
      super(context, profileName);
    }

    @Override
    public RetryVerdict onReadTimeoutVerdict(
        @NonNull Request request,
        @NonNull ConsistencyLevel cl,
        int blockFor,
        int received,
        boolean dataPresent,
        int retryCount) {
      if (verdict != null) {
        return verdict;
      }
      return super.onReadTimeoutVerdict(request, cl, blockFor, received, dataPresent, retryCount);
    }

    @Override
    public RetryVerdict onWriteTimeoutVerdict(
        @NonNull Request request,
        @NonNull ConsistencyLevel cl,
        @NonNull WriteType writeType,
        int blockFor,
        int received,
        int retryCount) {
      if (verdict != null) {
        return verdict;
      }
      return super.onWriteTimeoutVerdict(request, cl, writeType, blockFor, received, retryCount);
    }

    @Override
    public RetryVerdict onUnavailableVerdict(
        @NonNull Request request,
        @NonNull ConsistencyLevel cl,
        int required,
        int alive,
        int retryCount) {
      if (verdict != null) {
        return verdict;
      }
      return super.onUnavailableVerdict(request, cl, required, alive, retryCount);
    }

    @Override
    public RetryVerdict onRequestAbortedVerdict(
        @NonNull Request request, @NonNull Throwable error, int retryCount) {
      if (verdict != null) {
        return verdict;
      }
      return super.onRequestAbortedVerdict(request, error, retryCount);
    }

    @Override
    public RetryVerdict onErrorResponseVerdict(
        @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
      if (verdict != null) {
        return verdict;
      }
      return super.onErrorResponseVerdict(request, error, retryCount);
    }
  }
}
