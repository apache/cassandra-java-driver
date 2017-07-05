/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.time;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.time.Clock;
import java.time.Duration;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;

abstract class MonotonicTimestampGeneratorTestBase {

  protected static final DriverOption WARNING_THRESHOLD_OPTION =
      CoreDriverOption.TIMESTAMP_GENERATOR_ROOT.concat(
          CoreDriverOption.RELATIVE_TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD);
  protected static final DriverOption WARNING_INTERVAL_OPTION =
      CoreDriverOption.TIMESTAMP_GENERATOR_ROOT.concat(
          CoreDriverOption.RELATIVE_TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL);

  @Mock protected Clock clock;
  @Mock protected InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock protected DriverConfigProfile defaultConfigProfile;

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  private Logger logger;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(config.defaultProfile()).thenReturn(defaultConfigProfile);
    Mockito.when(context.config()).thenReturn(config);

    // Disable warnings by default
    Mockito.when(defaultConfigProfile.isDefined(WARNING_THRESHOLD_OPTION)).thenReturn(true);
    Mockito.when(defaultConfigProfile.getDuration(WARNING_THRESHOLD_OPTION))
        .thenReturn(Duration.ofNanos(0));
    // Actual value doesn't really matter since we only test the first warning
    Mockito.when(defaultConfigProfile.getDuration(WARNING_INTERVAL_OPTION))
        .thenReturn(Duration.ofSeconds(10));

    logger = (Logger) LoggerFactory.getLogger(MonotonicTimestampGenerator.class);
    logger.addAppender(appender);
  }

  @AfterMethod
  public void teardown() {
    logger.detachAppender(appender);
  }

  protected abstract MonotonicTimestampGenerator newInstance(Clock clock);

  @Test
  public void should_use_clock_if_it_keeps_increasing() {
    OngoingStubbing<Long> stub = Mockito.when(clock.currentTimeMicros());
    for (long l = 1; l < 5; l++) {
      stub = stub.thenReturn(l);
    }

    MonotonicTimestampGenerator generator = newInstance(clock);

    for (long l = 1; l < 5; l++) {
      assertThat(generator.next()).isEqualTo(l);
    }
  }

  @Test
  public void should_increment_if_clock_does_not_increase() {
    Mockito.when(clock.currentTimeMicros()).thenReturn(1L, 1L, 1L, 5L);

    MonotonicTimestampGenerator generator = newInstance(clock);

    assertThat(generator.next()).isEqualTo(1);
    assertThat(generator.next()).isEqualTo(2);
    assertThat(generator.next()).isEqualTo(3);
    assertThat(generator.next()).isEqualTo(5);
  }

  @Test
  public void should_warn_if_timestamps_drift() {
    Mockito.when(defaultConfigProfile.getDuration(WARNING_THRESHOLD_OPTION))
        .thenReturn(Duration.ofNanos(2 * 1000));
    Mockito.when(clock.currentTimeMicros()).thenReturn(1L, 1L, 1L, 1L, 1L);

    MonotonicTimestampGenerator generator = newInstance(clock);

    assertThat(generator.next()).isEqualTo(1);
    assertThat(generator.next()).isEqualTo(2);
    assertThat(generator.next()).isEqualTo(3);
    assertThat(generator.next()).isEqualTo(4);
    // Clock still at 1, last returned timestamp is 4 (> 1 + 2), should warn
    assertThat(generator.next()).isEqualTo(5);

    Mockito.verify(appender).doAppend(loggingEventCaptor.capture());
    ILoggingEvent log = loggingEventCaptor.getValue();
    assertThat(log.getLevel()).isEqualTo(Level.WARN);
    assertThat(log.getMessage()).contains("Clock skew detected");
  }

  @Test
  public void should_go_back_to_clock_if_new_tick_high_enough() {
    Mockito.when(clock.currentTimeMicros()).thenReturn(1L, 1L, 1L, 1L, 1L, 10L);

    MonotonicTimestampGenerator generator = newInstance(clock);

    for (long l = 1; l <= 5; l++) {
      // Clock at 1, keep incrementing
      assertThat(generator.next()).isEqualTo(l);
    }

    // Last returned is 5, but clock has ticked to 10, should use that.
    assertThat(generator.next()).isEqualTo(10);
  }
}
