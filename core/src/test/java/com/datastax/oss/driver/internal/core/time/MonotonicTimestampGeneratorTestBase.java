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
package com.datastax.oss.driver.internal.core.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.LoggerFactory;

abstract class MonotonicTimestampGeneratorTestBase {

  @Mock protected Clock clock;
  @Mock protected InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock protected DriverExecutionProfile defaultProfile;

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  private Logger logger;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(context.getConfig()).thenReturn(config);

    // Disable warnings by default
    when(defaultProfile.getDuration(
            DefaultDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD, Duration.ZERO))
        .thenReturn(Duration.ZERO);
    // Actual value doesn't really matter since we only test the first warning
    when(defaultProfile.getDuration(DefaultDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL))
        .thenReturn(Duration.ofSeconds(10));

    logger = (Logger) LoggerFactory.getLogger(MonotonicTimestampGenerator.class);
    logger.addAppender(appender);
  }

  @After
  public void teardown() {
    logger.detachAppender(appender);
  }

  protected abstract MonotonicTimestampGenerator newInstance(Clock clock);

  @Test
  public void should_use_clock_if_it_keeps_increasing() {
    OngoingStubbing<Long> stub = when(clock.currentTimeMicros());
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
    when(clock.currentTimeMicros()).thenReturn(1L, 1L, 1L, 5L);

    MonotonicTimestampGenerator generator = newInstance(clock);

    assertThat(generator.next()).isEqualTo(1);
    assertThat(generator.next()).isEqualTo(2);
    assertThat(generator.next()).isEqualTo(3);
    assertThat(generator.next()).isEqualTo(5);
  }

  @Test
  public void should_warn_if_timestamps_drift() {
    when(defaultProfile.getDuration(
            DefaultDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD, Duration.ZERO))
        .thenReturn(Duration.ofNanos(2 * 1000));
    when(clock.currentTimeMicros()).thenReturn(1L, 1L, 1L, 1L, 1L);

    MonotonicTimestampGenerator generator = newInstance(clock);

    assertThat(generator.next()).isEqualTo(1);
    assertThat(generator.next()).isEqualTo(2);
    assertThat(generator.next()).isEqualTo(3);
    assertThat(generator.next()).isEqualTo(4);
    // Clock still at 1, last returned timestamp is 4 (> 1 + 2), should warn
    assertThat(generator.next()).isEqualTo(5);

    verify(appender).doAppend(loggingEventCaptor.capture());
    ILoggingEvent log = loggingEventCaptor.getValue();
    assertThat(log.getLevel()).isEqualTo(Level.WARN);
    assertThat(log.getMessage()).contains("Clock skew detected");
  }

  @Test
  public void should_go_back_to_clock_if_new_tick_high_enough() {
    when(clock.currentTimeMicros()).thenReturn(1L, 1L, 1L, 1L, 1L, 10L);

    MonotonicTimestampGenerator generator = newInstance(clock);

    for (long l = 1; l <= 5; l++) {
      // Clock at 1, keep incrementing
      assertThat(generator.next()).isEqualTo(l);
    }

    // Last returned is 5, but clock has ticked to 10, should use that.
    assertThat(generator.next()).isEqualTo(10);
  }
}
