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
package com.datastax.oss.driver.internal.core.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.context.DriverContext;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ExponentialReconnectionPolicyTest {

  @Mock private DriverContext driverContext;
  @Mock private DriverConfig driverConfig;
  @Mock private DriverExecutionProfile profile;
  private final long baseDelay = 1000L;
  private final long maxDelay = 60000L;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(driverConfig.getDefaultProfile()).thenReturn(profile);
    when(driverContext.getConfig()).thenReturn(driverConfig);
    when(profile.getDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY))
        .thenReturn(Duration.of(baseDelay, ChronoUnit.MILLIS));
    when(profile.getDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY))
        .thenReturn(Duration.of(maxDelay, ChronoUnit.MILLIS));
  }

  @Test
  public void should_generate_exponential_delay_with_jitter() throws Exception {
    ExponentialReconnectionPolicy policy = new ExponentialReconnectionPolicy(driverContext);
    ReconnectionPolicy.ReconnectionSchedule schedule = policy.newControlConnectionSchedule(false);
    // generate a number of delays and make sure they are all within the base/max values range
    // limit the loop to 53 as the bit shift and min/max calculations will cause long overflows
    // past that
    for (int i = 0; i < 54; ++i) {
      // compute the min and max delays based on attempt count (i) and prevent long overflows
      long exponentialDelay = Math.min(baseDelay * (1L << i), maxDelay);
      // min will be 85% of the pure exponential delay (with a floor of baseDelay)
      long minJitterDelay = Math.max(baseDelay, (exponentialDelay * 85) / 100);
      // max will be 115% of the pure exponential delay (with a ceiling of maxDelay)
      long maxJitterDelay = Math.min(maxDelay, (exponentialDelay * 115) / 100);
      long delay = schedule.nextDelay().toMillis();
      assertThat(delay).isBetween(minJitterDelay, maxJitterDelay);
    }
  }
}
