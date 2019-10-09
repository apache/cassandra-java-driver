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
package com.datastax.dse.driver.internal.core.insights;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.internal.core.insights.schema.ReconnectionPolicyInfo;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
import java.time.Duration;
import org.assertj.core.data.MapEntry;
import org.junit.Test;

public class ReconnectionPolicyInfoFinderTest {

  @Test
  public void should_find_an_info_about_constant_reconnection_policy() {
    // given
    DriverExecutionProfile driverExecutionProfile = mock(DriverExecutionProfile.class);
    when(driverExecutionProfile.getDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY))
        .thenReturn(Duration.ofMillis(100));
    ReconnectionPolicy constantReconnectionPolicy = mock(ConstantReconnectionPolicy.class);

    // when
    ReconnectionPolicyInfo reconnectionPolicyInfo =
        new ReconnectionPolicyInfoFinder()
            .getReconnectionPolicyInfo(constantReconnectionPolicy, driverExecutionProfile);

    // then
    assertThat(reconnectionPolicyInfo.getOptions()).contains(MapEntry.entry("delayMs", 100L));
    assertThat(reconnectionPolicyInfo.getType()).contains("ConstantReconnectionPolicy");
  }

  @Test
  public void should_find_an_info_about_exponential_reconnection_policy() {
    ExponentialReconnectionPolicy exponentialReconnectionPolicy =
        mock(ExponentialReconnectionPolicy.class);
    when(exponentialReconnectionPolicy.getBaseDelayMs()).thenReturn(100L);
    when(exponentialReconnectionPolicy.getMaxAttempts()).thenReturn(10L);
    when(exponentialReconnectionPolicy.getMaxDelayMs()).thenReturn(200L);

    // when
    ReconnectionPolicyInfo reconnectionPolicyInfo =
        new ReconnectionPolicyInfoFinder()
            .getReconnectionPolicyInfo(exponentialReconnectionPolicy, null);

    // then
    assertThat(reconnectionPolicyInfo.getOptions()).contains(MapEntry.entry("baseDelayMs", 100L));
    assertThat(reconnectionPolicyInfo.getOptions()).contains(MapEntry.entry("maxAttempts", 10L));
    assertThat(reconnectionPolicyInfo.getOptions()).contains(MapEntry.entry("maxDelayMs", 200L));
    assertThat(reconnectionPolicyInfo.getType()).contains("ExponentialReconnectionPolicy");
  }
}
