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
package com.datastax.oss.driver.internal.metrics.micrometer;

import static com.datastax.oss.driver.internal.metrics.micrometer.MicrometerMetricsFactory.LOWEST_ACCEPTABLE_EXPIRE_AFTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class MicrometerMetricsFactoryTest {

  private static final String LOG_PREFIX = "prefix";

  @Test
  public void should_log_warning_when_provided_eviction_time_setting_is_too_low() {
    // given
    Duration expireAfter = LOWEST_ACCEPTABLE_EXPIRE_AFTER.minusMinutes(1);
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(MicrometerMetricsFactory.class, Level.WARN);
    DriverExecutionProfile driverExecutionProfile = mock(DriverExecutionProfile.class);

    // when
    when(driverExecutionProfile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(expireAfter);
    MicrometerMetricsFactory.getAndValidateEvictionTime(driverExecutionProfile, LOG_PREFIX);

    // then
    verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
    assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
    assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
        .contains(
            String.format(
                "[%s] Value too low for %s: %s. Forcing to %s instead.",
                LOG_PREFIX,
                DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER.getPath(),
                expireAfter,
                LOWEST_ACCEPTABLE_EXPIRE_AFTER));
  }

  @Test
  @UseDataProvider(value = "acceptableEvictionTimes")
  public void should_not_log_warning_when_provided_eviction_time_setting_is_acceptable(
      Duration expireAfter) {
    // given
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(MicrometerMetricsFactory.class, Level.WARN);
    DriverExecutionProfile driverExecutionProfile = mock(DriverExecutionProfile.class);

    // when
    when(driverExecutionProfile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(expireAfter);
    MicrometerMetricsFactory.getAndValidateEvictionTime(driverExecutionProfile, LOG_PREFIX);

    // then
    verify(logger.appender, timeout(500).times(0)).doAppend(logger.loggingEventCaptor.capture());
  }

  @DataProvider
  public static Object[][] acceptableEvictionTimes() {
    return new Object[][] {
      {LOWEST_ACCEPTABLE_EXPIRE_AFTER}, {LOWEST_ACCEPTABLE_EXPIRE_AFTER.plusMinutes(1)}
    };
  }
}
