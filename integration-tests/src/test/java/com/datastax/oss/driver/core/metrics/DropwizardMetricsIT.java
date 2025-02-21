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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricId;
import com.datastax.oss.driver.internal.core.metrics.MetricIdGenerator;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.ArrayList;
import java.util.List;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class DropwizardMetricsIT extends MetricsITBase {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  @Override
  protected SimulacronRule simulacron() {
    return SIMULACRON_RULE;
  }

  @Override
  protected MetricRegistry newMetricRegistry() {
    return new MetricRegistry();
  }

  @Override
  protected String getMetricsFactoryClass() {
    return "DropwizardMetricsFactory";
  }

  @Override
  protected void assertMetricsPresent(CqlSession session) {

    MetricRegistry registry =
        (MetricRegistry) ((InternalDriverContext) session.getContext()).getMetricRegistry();
    assertThat(registry).isNotNull();

    assertThat(registry.getMetrics())
        .hasSize(ENABLED_SESSION_METRICS.size() + ENABLED_NODE_METRICS.size() * 3);

    MetricIdGenerator metricIdGenerator =
        ((InternalDriverContext) session.getContext()).getMetricIdGenerator();

    assertThat(session.getMetrics()).isPresent();
    Metrics metrics = session.getMetrics().get();

    for (DefaultSessionMetric metric : ENABLED_SESSION_METRICS) {

      MetricId id = metricIdGenerator.sessionMetricId(metric);
      Metric m = registry.getMetrics().get(id.getName());
      assertThat(m).isNotNull();

      // assert that the same metric is retrievable through the registry and through the driver API
      assertThat(metrics.getSessionMetric(metric))
          .isPresent()
          .hasValueSatisfying(v -> assertThat(v).isSameAs(m));

      switch (metric) {
        case CONNECTED_NODES:
          assertThat(m).isInstanceOf(Gauge.class);
          assertThat((Integer) ((Gauge<?>) m).getValue()).isEqualTo(3);
          break;
        case CQL_REQUESTS:
        case SEND_LATENCY:
          assertThat(m).isInstanceOf(Timer.class);
          await().untilAsserted(() -> assertThat(((Timer) m).getCount()).isEqualTo(30));
          break;
        case CQL_PREPARED_CACHE_SIZE:
          assertThat(m).isInstanceOf(Gauge.class);
          assertThat((Long) ((Gauge<?>) m).getValue()).isOne();
          break;
        case BYTES_SENT:
        case BYTES_RECEIVED:
          assertThat(m).isInstanceOf(Meter.class);
          assertThat(((Meter) m).getCount()).isGreaterThan(0);
          break;
        case CQL_CLIENT_TIMEOUTS:
        case THROTTLING_ERRORS:
          assertThat(m).isInstanceOf(Counter.class);
          assertThat(((Counter) m).getCount()).isZero();
          break;
        case THROTTLING_DELAY:
          assertThat(m).isInstanceOf(Timer.class);
          assertThat(((Timer) m).getCount()).isZero();
          break;
        case THROTTLING_QUEUE_SIZE:
          assertThat(m).isInstanceOf(Gauge.class);
          assertThat((Integer) ((Gauge<?>) m).getValue()).isZero();
          break;
      }
    }

    for (Node node : session.getMetadata().getNodes().values()) {

      for (DefaultNodeMetric metric : ENABLED_NODE_METRICS) {

        MetricId id = metricIdGenerator.nodeMetricId(node, metric);
        Metric m = registry.getMetrics().get(id.getName());
        assertThat(m).isNotNull();

        // assert that the same metric is retrievable through the registry and through the driver
        // API
        assertThat(metrics.getNodeMetric(node, metric))
            .isPresent()
            .hasValueSatisfying(v -> assertThat(v).isSameAs(m));

        switch (metric) {
          case OPEN_CONNECTIONS:
            assertThat(m).isInstanceOf(Gauge.class);
            // control node has 2 connections
            assertThat((Integer) ((Gauge<?>) m).getValue()).isBetween(1, 2);
            break;
          case CQL_MESSAGES:
            assertThat(m).isInstanceOf(Timer.class);
            await().untilAsserted(() -> assertThat(((Timer) m).getCount()).isEqualTo(10));
            break;
          case READ_TIMEOUTS:
          case WRITE_TIMEOUTS:
          case UNAVAILABLES:
          case OTHER_ERRORS:
          case ABORTED_REQUESTS:
          case UNSENT_REQUESTS:
          case RETRIES:
          case IGNORES:
          case RETRIES_ON_READ_TIMEOUT:
          case RETRIES_ON_WRITE_TIMEOUT:
          case RETRIES_ON_UNAVAILABLE:
          case RETRIES_ON_OTHER_ERROR:
          case RETRIES_ON_ABORTED:
          case IGNORES_ON_READ_TIMEOUT:
          case IGNORES_ON_WRITE_TIMEOUT:
          case IGNORES_ON_UNAVAILABLE:
          case IGNORES_ON_OTHER_ERROR:
          case IGNORES_ON_ABORTED:
          case SPECULATIVE_EXECUTIONS:
          case CONNECTION_INIT_ERRORS:
          case AUTHENTICATION_ERRORS:
            assertThat(m).isInstanceOf(Counter.class);
            assertThat(((Counter) m).getCount()).isZero();
            break;
          case BYTES_SENT:
          case BYTES_RECEIVED:
            assertThat(m).isInstanceOf(Meter.class);
            assertThat(((Meter) m).getCount()).isGreaterThan(0L);
            break;
          case AVAILABLE_STREAMS:
          case IN_FLIGHT:
          case ORPHANED_STREAMS:
            assertThat(m).isInstanceOf(Gauge.class);
            break;
        }
      }
    }
  }

  @Override
  protected void assertNodeMetricsNotEvicted(CqlSession session, Node node) {
    InternalDriverContext context = (InternalDriverContext) session.getContext();
    MetricRegistry registry = (MetricRegistry) context.getMetricRegistry();
    assertThat(registry).isNotNull();
    for (String id : nodeMetricIds(context, node)) {
      assertThat(registry.getMetrics()).containsKey(id);
    }
  }

  @Override
  protected void assertMetricsNotPresent(Object registry) {
    MetricRegistry dropwizardRegistry = (MetricRegistry) registry;
    assertThat(dropwizardRegistry.getMetrics()).isEmpty();
  }

  @Override
  protected void assertNodeMetricsEvicted(CqlSession session, Node node) {
    InternalDriverContext context = (InternalDriverContext) session.getContext();
    MetricRegistry registry = (MetricRegistry) context.getMetricRegistry();
    assertThat(registry).isNotNull();
    for (String id : nodeMetricIds(context, node)) {
      assertThat(registry.getMetrics()).doesNotContainKey(id);
    }
  }

  private List<String> nodeMetricIds(InternalDriverContext context, Node node) {
    List<String> ids = new ArrayList<>();
    for (DefaultNodeMetric metric : ENABLED_NODE_METRICS) {
      MetricId id = context.getMetricIdGenerator().nodeMetricId(node, metric);
      ids.add(id.getName());
    }
    return ids;
  }
}
