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
package com.datastax.oss.driver.api.core.specex;

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.specex.ConstantSpeculativeExecutionPolicy;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConstantSpeculativeExecutionPolicyTest {
  @Mock private DriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile defaultProfile;
  @Mock private Request request;

  @Before
  public void setup() {
    Mockito.when(context.config()).thenReturn(config);
    Mockito.when(config.getProfile(DriverConfigProfile.DEFAULT_NAME)).thenReturn(defaultProfile);
  }

  private void mockOptions(int maxExecutions, long constantDelayMillis) {
    Mockito.when(defaultProfile.getInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX))
        .thenReturn(maxExecutions);
    Mockito.when(defaultProfile.getDuration(DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY))
        .thenReturn(Duration.ofMillis(constantDelayMillis));
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_if_delay_negative() {
    mockOptions(1, -10);
    new ConstantSpeculativeExecutionPolicy(context, DriverConfigProfile.DEFAULT_NAME);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_if_max_less_than_one() {
    mockOptions(0, 10);
    new ConstantSpeculativeExecutionPolicy(context, DriverConfigProfile.DEFAULT_NAME);
  }

  @Test
  public void should_return_delay_until_max() {
    mockOptions(3, 10);
    SpeculativeExecutionPolicy policy =
        new ConstantSpeculativeExecutionPolicy(context, DriverConfigProfile.DEFAULT_NAME);

    // Initial execution starts, schedule first speculative execution
    assertThat(policy.nextExecution(null, null, request, 1)).isEqualTo(10);
    // First speculative execution starts, schedule second one
    assertThat(policy.nextExecution(null, null, request, 2)).isEqualTo(10);
    // Second speculative execution starts, we're at 3 => stop
    assertThat(policy.nextExecution(null, null, request, 3)).isNegative();
  }
}
