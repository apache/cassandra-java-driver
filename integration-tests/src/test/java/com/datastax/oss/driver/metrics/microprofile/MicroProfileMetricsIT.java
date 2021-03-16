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
package com.datastax.oss.driver.metrics.microprofile;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricId;
import com.datastax.oss.driver.internal.core.metrics.MetricIdGenerator;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.metrics.micrometer.MicrometerNodeMetricUpdater;
import com.datastax.oss.driver.internal.metrics.microprofile.MicroProfileMetricsFactory;
import com.datastax.oss.driver.internal.metrics.microprofile.MicroProfileTags;
import com.datastax.oss.driver.metrics.common.AbstractMetricsTestBase;
import com.datastax.oss.driver.shaded.guava.common.base.Ticker;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import io.smallrye.metrics.MetricsRegistryImpl;
import java.lang.reflect.Field;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.Gauge;
import org.eclipse.microprofile.metrics.Meter;
import org.eclipse.microprofile.metrics.Metric;
import org.eclipse.microprofile.metrics.MetricID;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Tag;
import org.eclipse.microprofile.metrics.Timer;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class MicroProfileMetricsIT extends AbstractMetricsTestBase {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  @Override
  protected SimulacronRule simulacron() {
    return SIMULACRON_RULE;
  }

  @Override
  protected MetricRegistry newMetricRegistry() {
    return new MetricsRegistryImpl();
  }

  @Override
  protected String getMetricsFactoryClass() {
    return "MicroProfileMetricsFactory";
  }

  @Override
  protected MetricsFactory newTickingMetricsFactory(InternalDriverContext context, Ticker ticker) {
    return new MicroProfileMetricsFactory(context, ticker);
  }

  @Override
  protected void assertMetrics(CqlSession session) {

    MetricRegistry registry =
        (MetricRegistry) ((InternalDriverContext) session.getContext()).getMetricRegistry();
    assertThat(registry).isNotNull();

    assertThat(registry.getMetrics())
        .hasSize(ENABLED_SESSION_METRICS.size() + ENABLED_NODE_METRICS.size() * 3);

    MetricIdGenerator metricIdGenerator =
        ((InternalDriverContext) session.getContext()).getMetricIdGenerator();

    for (DefaultSessionMetric metric : ENABLED_SESSION_METRICS) {
      MetricId metricId = metricIdGenerator.sessionMetricId(metric);
      Tag[] tags = MicroProfileTags.toMicroProfileTags(metricId.getTags());
      MetricID id = new MetricID(metricId.getName(), tags);
      Metric m = registry.getMetrics().get(id);
      assertThat(m).isNotNull();
      switch (metric) {
        case BYTES_SENT:
        case BYTES_RECEIVED:
          assertThat(m).isInstanceOf(Meter.class);
          assertThat(((Meter) m).getCount()).isGreaterThan(0L);
          break;
        case CONNECTED_NODES:
          assertThat(m).isInstanceOf(Gauge.class);
          assertThat((Integer) ((Gauge<?>) m).getValue()).isEqualTo(3);
          break;
        case CQL_REQUESTS:
          assertThat(m).isInstanceOf(Timer.class);
          assertThat(((Timer) m).getCount()).isEqualTo(30L);
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
        case CQL_PREPARED_CACHE_SIZE:
          assertThat(m).isInstanceOf(Gauge.class);
          assertThat((Long) ((Gauge<?>) m).getValue()).isZero();
          break;
      }
    }

    for (Node node : session.getMetadata().getNodes().values()) {

      for (DefaultNodeMetric metric : ENABLED_NODE_METRICS) {
        MetricId description = metricIdGenerator.nodeMetricId(node, metric);
        Tag[] tags = MicroProfileTags.toMicroProfileTags(description.getTags());
        MetricID id = new MetricID(description.getName(), tags);
        Metric m = registry.getMetrics().get(id);
        assertThat(m).isNotNull();
        switch (metric) {
          case OPEN_CONNECTIONS:
            assertThat(m).isInstanceOf(Gauge.class);
            // control node has 2 connections
            assertThat((Integer) ((Gauge<?>) m).getValue()).isBetween(1, 2);
            break;
          case CQL_MESSAGES:
            assertThat(m).isInstanceOf(Timer.class);
            assertThat(((Timer) m).getCount()).isEqualTo(10L);
            break;
          case AVAILABLE_STREAMS:
            assertThat(m).isInstanceOf(Gauge.class);
            assertThat((Integer) ((Gauge<?>) m).getValue()).isGreaterThan(100);
            break;
          case IN_FLIGHT:
            assertThat(m).isInstanceOf(Gauge.class);
            break;
          case ORPHANED_STREAMS:
            assertThat(m).isInstanceOf(Gauge.class);
            assertThat((Integer) ((Gauge<?>) m).getValue()).isZero();
            break;
          case BYTES_SENT:
          case BYTES_RECEIVED:
            assertThat(m).isInstanceOf(Meter.class);
            assertThat(((Meter) m).getCount()).isGreaterThan(0L);
            break;
          case UNSENT_REQUESTS:
          case ABORTED_REQUESTS:
          case WRITE_TIMEOUTS:
          case READ_TIMEOUTS:
          case UNAVAILABLES:
          case OTHER_ERRORS:
          case RETRIES:
          case RETRIES_ON_ABORTED:
          case RETRIES_ON_READ_TIMEOUT:
          case RETRIES_ON_WRITE_TIMEOUT:
          case RETRIES_ON_UNAVAILABLE:
          case RETRIES_ON_OTHER_ERROR:
          case IGNORES:
          case IGNORES_ON_ABORTED:
          case IGNORES_ON_READ_TIMEOUT:
          case IGNORES_ON_WRITE_TIMEOUT:
          case IGNORES_ON_UNAVAILABLE:
          case IGNORES_ON_OTHER_ERROR:
          case SPECULATIVE_EXECUTIONS:
          case CONNECTION_INIT_ERRORS:
          case AUTHENTICATION_ERRORS:
            assertThat(m).isInstanceOf(Counter.class);
            assertThat(((Counter) m).getCount()).isZero();
            break;
        }
      }
    }
  }

  @Override
  protected void assertNodeMetricsNotEvicted(CqlSession session, Node node) throws Exception {
    InternalDriverContext context = (InternalDriverContext) session.getContext();
    MetricIdGenerator metricIdGenerator = context.getMetricIdGenerator();
    MetricRegistry registry = (MetricRegistry) context.getMetricRegistry();
    assertThat(registry).isNotNull();
    triggerCacheCleanup(context.getMetricsFactory());
    for (DefaultNodeMetric metric : ENABLED_NODE_METRICS) {
      MetricId id = metricIdGenerator.nodeMetricId(node, metric);
      Tag[] tags = MicroProfileTags.toMicroProfileTags(id.getTags());
      assertThat(registry.getMetrics()).containsKey(new MetricID(id.getName(), tags));
    }
  }

  @Override
  protected void assertNodeMetricsEvicted(CqlSession session, Node node) throws Exception {
    InternalDriverContext context = (InternalDriverContext) session.getContext();
    MetricIdGenerator metricIdGenerator = context.getMetricIdGenerator();
    MetricRegistry registry = (MetricRegistry) context.getMetricRegistry();
    assertThat(registry).isNotNull();
    triggerCacheCleanup(context.getMetricsFactory());
    for (DefaultNodeMetric metric : ENABLED_NODE_METRICS) {
      MetricId id = metricIdGenerator.nodeMetricId(node, metric);
      Tag[] tags = MicroProfileTags.toMicroProfileTags(id.getTags());
      assertThat(registry.getMetrics()).doesNotContainKey(new MetricID(id.getName(), tags));
    }
  }

  private void triggerCacheCleanup(MetricsFactory metricsFactory) throws Exception {
    Field metricsCache = MicroProfileMetricsFactory.class.getDeclaredField("metricsCache");
    metricsCache.setAccessible(true);
    @SuppressWarnings("unchecked")
    Cache<Node, MicrometerNodeMetricUpdater> cache =
        (Cache<Node, MicrometerNodeMetricUpdater>) metricsCache.get(metricsFactory);
    cache.cleanUp();
  }
}
