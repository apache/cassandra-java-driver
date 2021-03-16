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
package com.datastax.oss.driver.metrics.common;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.core.metrics.FakeTicker;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.DefaultMetricIdGenerator;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.metrics.TaggingMetricIdGenerator;
import com.datastax.oss.driver.shaded.guava.common.base.Ticker;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public abstract class AbstractMetricsTestBase {

  protected static final List<DefaultSessionMetric> ENABLED_SESSION_METRICS =
      Arrays.asList(DefaultSessionMetric.values());

  protected static final List<DefaultNodeMetric> ENABLED_NODE_METRICS =
      Arrays.asList(DefaultNodeMetric.values());

  protected abstract SimulacronRule simulacron();

  protected abstract Object newMetricRegistry();

  protected abstract String getMetricsFactoryClass();

  protected abstract MetricsFactory newTickingMetricsFactory(
      InternalDriverContext context, Ticker ticker);

  protected abstract void assertMetrics(CqlSession session);

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
  public void should_expose_metrics(Class<?> descriptorClass, String prefix) {

    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
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
            .withString(DefaultDriverOption.METRICS_FACTORY_CLASS, getMetricsFactoryClass())
            .withString(
                DefaultDriverOption.METRICS_ID_GENERATOR_CLASS, descriptorClass.getSimpleName())
            .withString(DefaultDriverOption.METRICS_ID_GENERATOR_PREFIX, prefix)
            .build();

    try (CqlSession session =
        CqlSession.builder()
            .addContactEndPoints(simulacron().getContactPoints())
            .withConfigLoader(loader)
            .withMetricRegistry(newMetricRegistry())
            .build()) {

      for (Node node : session.getMetadata().getNodes().values()) {
        for (int i = 0; i < 10; i++) {
          session.execute(
              SimpleStatement.newInstance("SELECT release_version FROM system.local")
                  .setNode(node));
        }
      }

      assertMetrics(session);
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
  public void should_evict_node_level_metrics() throws Exception {
    // given
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
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
            .withDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER, Duration.ofHours(1))
            .build();
    FakeTicker fakeTicker = new FakeTicker();
    try (CqlSession session =
        new TestSessionBuilder()
            .addContactEndPoints(simulacron().getContactPoints())
            .withConfigLoader(loader)
            .withMetricRegistry(newMetricRegistry())
            .withTicker(fakeTicker)
            .build()) {

      for (Node node : session.getMetadata().getNodes().values()) {
        for (int i = 0; i < 10; i++) {
          session.execute(
              SimpleStatement.newInstance("SELECT release_version FROM system.local")
                  .setNode(node));
        }
      }

      Node node1 = findNode(session, 0);
      Node node2 = findNode(session, 1);
      Node node3 = findNode(session, 2);

      // when advance time to before eviction
      fakeTicker.advance(Duration.ofMinutes(59));
      // execute query that updates only node1
      session.execute(
          SimpleStatement.newInstance("SELECT release_version FROM system.local").setNode(node1));
      // advance time to after eviction
      fakeTicker.advance(Duration.ofMinutes(2));

      // then no node-level metrics should be evicted from node1
      assertNodeMetricsNotEvicted(session, node1);
      // node2 and node3 metrics should have been evicted
      assertNodeMetricsEvicted(session, node2);
      assertNodeMetricsEvicted(session, node3);
    }
  }

  private Node findNode(CqlSession session, int id) {
    InetSocketAddress address1 = simulacron().cluster().node(id).inetSocketAddress();
    return session.getMetadata().findNode(address1).orElseThrow(IllegalStateException::new);
  }

  private class TestSessionBuilder extends SessionBuilder<TestSessionBuilder, CqlSession> {

    private Ticker ticker;

    @Override
    protected CqlSession wrap(@NonNull CqlSession defaultSession) {
      return defaultSession;
    }

    public TestSessionBuilder withTicker(Ticker ticker) {
      this.ticker = ticker;
      return this;
    }

    @Override
    protected DriverContext buildContext(
        DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
      return new TestDriverContext(configLoader, programmaticArguments, ticker);
    }
  }

  private class TestDriverContext extends DefaultDriverContext {

    private final Ticker ticker;

    public TestDriverContext(
        @NonNull DriverConfigLoader configLoader,
        @NonNull ProgrammaticArguments programmaticArguments,
        @NonNull Ticker ticker) {
      super(configLoader, programmaticArguments);
      this.ticker = ticker;
    }

    @Override
    protected MetricsFactory buildMetricsFactory() {
      return newTickingMetricsFactory(this, ticker);
    }
  }
}
