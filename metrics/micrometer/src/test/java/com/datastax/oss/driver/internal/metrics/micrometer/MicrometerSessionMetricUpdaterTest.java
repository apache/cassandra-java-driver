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
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.DefaultMetricId;
import com.datastax.oss.driver.internal.core.metrics.MetricId;
import com.datastax.oss.driver.internal.core.metrics.MetricIdGenerator;
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
public class MicrometerSessionMetricUpdaterTest {

  private static final MetricId METRIC_ID = new DefaultMetricId("irrelevant", ImmutableMap.of());

  @Test
  @UseDataProvider(value = "timerMetrics")
  public void should_create_timer(
      SessionMetric metric,
      DriverOption lowest,
      DriverOption highest,
      DriverOption digits,
      DriverOption sla,
      DriverOption percentiles) {
    // given
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    MetricIdGenerator generator = mock(MetricIdGenerator.class);
    Set<SessionMetric> enabledMetrics = Collections.singleton(metric);

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
    when(generator.sessionMetricId(metric)).thenReturn(METRIC_ID);

    SimpleMeterRegistry registry = spy(new SimpleMeterRegistry());
    MicrometerSessionMetricUpdater updater =
        new MicrometerSessionMetricUpdater(context, enabledMetrics, registry);

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
      SessionMetric metric,
      DriverOption lowest,
      DriverOption highest,
      DriverOption digits,
      DriverOption sla,
      DriverOption percentiles) {
    // given
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    MetricIdGenerator generator = mock(MetricIdGenerator.class);
    Set<SessionMetric> enabledMetrics = Collections.singleton(metric);

    // when
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(context.getMetricIdGenerator()).thenReturn(generator);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(Duration.ofHours(1));
    when(profile.isDefined(sla)).thenReturn(false);
    when(profile.getDurationList(sla))
        .thenReturn(Arrays.asList(Duration.ofMillis(100), Duration.ofMillis(500)));
    when(profile.getBoolean(DefaultDriverOption.METRICS_GENERATE_AGGREGABLE_HISTOGRAMS))
        .thenReturn(true);
    when(profile.isDefined(percentiles)).thenReturn(false);
    when(profile.getDoubleList(percentiles)).thenReturn(Arrays.asList(0.75, 0.95, 0.99));
    when(generator.sessionMetricId(metric)).thenReturn(METRIC_ID);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerSessionMetricUpdater updater =
        new MicrometerSessionMetricUpdater(context, enabledMetrics, registry);

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
        DefaultSessionMetric.CQL_REQUESTS,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_LOWEST,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_SLO,
        DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_PUBLISH_PERCENTILES,
      },
      {
        DseSessionMetric.GRAPH_REQUESTS,
        DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_LOWEST,
        DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_HIGHEST,
        DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_DIGITS,
        DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_SLO,
        DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_PUBLISH_PERCENTILES,
      },
      {
        DseSessionMetric.CONTINUOUS_CQL_REQUESTS,
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_LOWEST,
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_HIGHEST,
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_DIGITS,
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_SLO,
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_PUBLISH_PERCENTILES
      },
      {
        DefaultSessionMetric.THROTTLING_DELAY,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_LOWEST,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_SLO,
        DefaultDriverOption.METRICS_SESSION_THROTTLING_PUBLISH_PERCENTILES
      },
    };
  }
}
