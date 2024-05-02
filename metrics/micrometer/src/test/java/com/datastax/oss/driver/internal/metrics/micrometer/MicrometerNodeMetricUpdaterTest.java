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
package com.datastax.oss.driver.internal.metrics.micrometer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.metrics.DseNodeMetric;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.AbstractMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.DefaultMetricId;
import com.datastax.oss.driver.internal.core.metrics.MetricId;
import com.datastax.oss.driver.internal.core.metrics.MetricIdGenerator;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class MicrometerNodeMetricUpdaterTest {

  private static final MetricId METRIC_ID = new DefaultMetricId("irrelevant", ImmutableMap.of());

  @Test
  public void should_log_warning_when_provided_eviction_time_setting_is_too_low() {
    // given
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(AbstractMetricUpdater.class, Level.WARN);
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    MetricIdGenerator generator = mock(MetricIdGenerator.class);
    Set<NodeMetric> enabledMetrics = Collections.singleton(DefaultNodeMetric.CQL_MESSAGES);
    Duration expireAfter = AbstractMetricUpdater.MIN_EXPIRE_AFTER.minusMinutes(1);

    // when
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(context.getMetricIdGenerator()).thenReturn(generator);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(expireAfter);
    when(generator.nodeMetricId(node, DefaultNodeMetric.CQL_MESSAGES)).thenReturn(METRIC_ID);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST))
        .thenReturn(Duration.ofSeconds(10));
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_LOWEST))
        .thenReturn(Duration.ofMillis(1));
    when(profile.getInt(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS)).thenReturn(5);

    MicrometerNodeMetricUpdater updater =
        new MicrometerNodeMetricUpdater(node, context, enabledMetrics, new SimpleMeterRegistry());

    // then
    assertThat(updater.getExpireAfter()).isEqualTo(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
    assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
    assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
        .contains(
            String.format(
                "[prefix] Value too low for %s: %s. Forcing to %s instead.",
                DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER.getPath(),
                expireAfter,
                AbstractMetricUpdater.MIN_EXPIRE_AFTER));
  }

  @Test
  @UseDataProvider(value = "acceptableEvictionTimes")
  public void should_not_log_warning_when_provided_eviction_time_setting_is_acceptable(
      Duration expireAfter) {
    // given
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(AbstractMetricUpdater.class, Level.WARN);
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    MetricIdGenerator generator = mock(MetricIdGenerator.class);
    Set<NodeMetric> enabledMetrics = Collections.singleton(DefaultNodeMetric.CQL_MESSAGES);

    // when
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(context.getMetricIdGenerator()).thenReturn(generator);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(expireAfter);
    when(generator.nodeMetricId(node, DefaultNodeMetric.CQL_MESSAGES)).thenReturn(METRIC_ID);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST))
        .thenReturn(Duration.ofSeconds(10));
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_LOWEST))
        .thenReturn(Duration.ofMillis(1));
    when(profile.getInt(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS)).thenReturn(5);

    MicrometerNodeMetricUpdater updater =
        new MicrometerNodeMetricUpdater(node, context, enabledMetrics, new SimpleMeterRegistry());

    // then
    assertThat(updater.getExpireAfter()).isEqualTo(expireAfter);
    verify(logger.appender, timeout(500).times(0)).doAppend(logger.loggingEventCaptor.capture());
  }

  @DataProvider
  public static Object[][] acceptableEvictionTimes() {
    return new Object[][] {
      {AbstractMetricUpdater.MIN_EXPIRE_AFTER},
      {AbstractMetricUpdater.MIN_EXPIRE_AFTER.plusMinutes(1)}
    };
  }

  @Test
  @UseDataProvider(value = "timerMetrics")
  public void should_create_timer(
      NodeMetric metric,
      DriverOption lowest,
      DriverOption highest,
      DriverOption digits,
      DriverOption sla,
      DriverOption percentiles) {
    // given
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    MetricIdGenerator generator = mock(MetricIdGenerator.class);
    Set<NodeMetric> enabledMetrics = Collections.singleton(metric);

    // when
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(context.getMetricIdGenerator()).thenReturn(generator);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(Duration.ofHours(1));
    when(profile.getDuration(lowest)).thenReturn(Duration.ofMillis(10));
    when(profile.getDuration(highest)).thenReturn(Duration.ofSeconds(1));
    when(profile.getInt(digits)).thenReturn(5);
    when(profile.isDefined(sla)).thenReturn(true);
    when(profile.getDurationList(sla))
        .thenReturn(Arrays.asList(Duration.ofMillis(100), Duration.ofMillis(500)));
    when(profile.isDefined(percentiles)).thenReturn(true);
    when(profile.getDoubleList(percentiles)).thenReturn(Arrays.asList(0.75, 0.95, 0.99));
    when(generator.nodeMetricId(node, metric)).thenReturn(METRIC_ID);

    SimpleMeterRegistry registry = spy(new SimpleMeterRegistry());
    MicrometerNodeMetricUpdater updater =
        new MicrometerNodeMetricUpdater(node, context, enabledMetrics, registry);

    for (int i = 0; i < 10; i++) {
      updater.updateTimer(metric, null, 100, TimeUnit.MILLISECONDS);
    }

    // then
    Timer timer = registry.find(METRIC_ID.getName()).timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(10);
    HistogramSnapshot snapshot = timer.takeSnapshot();
    assertThat(snapshot.histogramCounts()).hasSize(2);
    assertThat(snapshot.percentileValues()).hasSize(3);
    assertThat(snapshot.percentileValues())
        .satisfiesExactlyInAnyOrder(
            valuePercentile -> assertThat(valuePercentile.percentile()).isEqualTo(0.75),
            valuePercentile -> assertThat(valuePercentile.percentile()).isEqualTo(0.95),
            valuePercentile -> assertThat(valuePercentile.percentile()).isEqualTo(0.99));
  }

  @Test
  @UseDataProvider(value = "timerMetrics")
  public void should_not_create_sla_percentiles(
      NodeMetric metric,
      DriverOption lowest,
      DriverOption highest,
      DriverOption digits,
      DriverOption sla,
      DriverOption percentiles) {
    // given
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    MetricIdGenerator generator = mock(MetricIdGenerator.class);
    Set<NodeMetric> enabledMetrics = Collections.singleton(metric);

    // when
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(context.getMetricIdGenerator()).thenReturn(generator);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(Duration.ofHours(1));
    when(profile.getDuration(lowest)).thenReturn(Duration.ofMillis(10));
    when(profile.getDuration(highest)).thenReturn(Duration.ofSeconds(1));
    when(profile.getInt(digits)).thenReturn(5);
    when(profile.isDefined(sla)).thenReturn(false);
    when(profile.getDurationList(sla))
        .thenReturn(Arrays.asList(Duration.ofMillis(100), Duration.ofMillis(500)));
    when(profile.isDefined(percentiles)).thenReturn(false);
    when(profile.getDoubleList(percentiles)).thenReturn(Arrays.asList(0.75, 0.95, 0.99));
    when(generator.nodeMetricId(node, metric)).thenReturn(METRIC_ID);

    SimpleMeterRegistry registry = spy(new SimpleMeterRegistry());
    MicrometerNodeMetricUpdater updater =
        new MicrometerNodeMetricUpdater(node, context, enabledMetrics, registry);

    for (int i = 0; i < 10; i++) {
      updater.updateTimer(metric, null, 100, TimeUnit.MILLISECONDS);
    }

    // then
    Timer timer = registry.find(METRIC_ID.getName()).timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(10);
    HistogramSnapshot snapshot = timer.takeSnapshot();
    assertThat(snapshot.histogramCounts()).hasSize(0);
    assertThat(snapshot.percentileValues()).hasSize(0);
  }

  @DataProvider
  public static Object[][] timerMetrics() {
    return new Object[][] {
      {
        DefaultNodeMetric.CQL_MESSAGES,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_LOWEST,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_SLO,
        DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_PUBLISH_PERCENTILES,
      },
      {
        DseNodeMetric.GRAPH_MESSAGES,
        DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_LOWEST,
        DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_HIGHEST,
        DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_DIGITS,
        DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_SLO,
        DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_PUBLISH_PERCENTILES,
      },
    };
  }
}
