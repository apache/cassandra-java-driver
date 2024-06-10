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
package com.datastax.oss.driver.internal.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DropwizardNodeMetricUpdaterTest {

  @Test
  public void should_log_warning_when_provided_eviction_time_setting_is_too_low() {
    // given
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(AbstractMetricUpdater.class, Level.WARN);
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    Set<NodeMetric> enabledMetrics = Collections.singleton(DefaultNodeMetric.CQL_MESSAGES);
    Duration expireAfter = AbstractMetricUpdater.MIN_EXPIRE_AFTER.minusMinutes(1);

    // when
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(expireAfter);

    DropwizardNodeMetricUpdater updater =
        new DropwizardNodeMetricUpdater(node, context, enabledMetrics, new MetricRegistry()) {
          @Override
          protected void initializeGauge(
              NodeMetric metric, DriverExecutionProfile profile, Supplier<Number> supplier) {
            // do nothing
          }

          @Override
          protected void initializeCounter(NodeMetric metric, DriverExecutionProfile profile) {
            // do nothing
          }

          @Override
          protected void initializeHdrTimer(
              NodeMetric metric,
              DriverExecutionProfile profile,
              DriverOption highestLatency,
              DriverOption significantDigits,
              DriverOption interval) {
            // do nothing
          }
        };

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
    Set<NodeMetric> enabledMetrics = Collections.singleton(DefaultNodeMetric.CQL_MESSAGES);

    // when
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(expireAfter);

    DropwizardNodeMetricUpdater updater =
        new DropwizardNodeMetricUpdater(node, context, enabledMetrics, new MetricRegistry()) {
          @Override
          protected void initializeGauge(
              NodeMetric metric, DriverExecutionProfile profile, Supplier<Number> supplier) {
            // do nothing
          }

          @Override
          protected void initializeCounter(NodeMetric metric, DriverExecutionProfile profile) {
            // do nothing
          }

          @Override
          protected void initializeHdrTimer(
              NodeMetric metric,
              DriverExecutionProfile profile,
              DriverOption highestLatency,
              DriverOption significantDigits,
              DriverOption interval) {
            // do nothing
          }
        };

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
}
