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
package com.datastax.oss.driver.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metrics.AbstractMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.DefaultMetricIdGenerator;
import com.datastax.oss.driver.internal.core.metrics.TaggingMetricIdGenerator;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assume;
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
  public void resetSimulacron() {
    simulacron().cluster().clearLogs();
    simulacron().cluster().clearPrimes(true);
  }

  @Test
  @UseDataProvider("descriptorsAndPrefixes")
  public void should_expose_metrics_if_enabled(Class<?> metricIdGenerator, String prefix) {

    Assume.assumeFalse(
        "Cannot use metric tags with Dropwizard",
        metricIdGenerator.getSimpleName().contains("Tagging")
            && getMetricsFactoryClass().contains("Dropwizard"));

    DriverConfigLoader loader =
        allMetricsEnabled()
            .withString(
                DefaultDriverOption.METRICS_ID_GENERATOR_CLASS, metricIdGenerator.getSimpleName())
            .withString(DefaultDriverOption.METRICS_ID_GENERATOR_PREFIX, prefix)
            .build();

    try (CqlSession session =
        CqlSession.builder()
            .addContactEndPoints(simulacron().getContactPoints())
            .withConfigLoader(loader)
            .withMetricRegistry(newMetricRegistry())
            .build()) {

      session.prepare("irrelevant");
      queryAllNodes(session);
      assertMetricsPresent(session);
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
}
