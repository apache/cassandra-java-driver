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
package com.datastax.oss.driver.internal.metrics.microprofile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.AbstractMetricUpdater;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Collections;
import java.util.List;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class MicroProfileMetricsFactoryTest {

  @Test
  @UseDataProvider(value = "invalidRegistryTypes")
  public void should_throw_if_wrong_or_missing_registry_type(
      Object registryObj, String expectedMsg) {
    // given
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    List<String> enabledMetrics =
        Collections.singletonList(DefaultSessionMetric.CQL_REQUESTS.getPath());
    // when
    when(config.getDefaultProfile()).thenReturn(profile);
    when(context.getConfig()).thenReturn(config);
    when(context.getSessionName()).thenReturn("MockSession");
    // registry object is not a registry type
    when(context.getMetricRegistry()).thenReturn(registryObj);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    when(profile.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED))
        .thenReturn(enabledMetrics);
    // then
    try {
      new MicroProfileMetricsFactory(context);
      fail(
          "MetricsFactory should require correct registry object type: "
              + MetricRegistry.class.getName());
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage()).isEqualTo(expectedMsg);
    }
  }

  @DataProvider
  public static Object[][] invalidRegistryTypes() {
    return new Object[][] {
      {
        Integer.MAX_VALUE,
        "Unexpected Metrics registry object. Expected registry object to be of type '"
            + MetricRegistry.class.getName()
            + "', but was '"
            + Integer.class.getName()
            + "'"
      },
      {
        null,
        "No metric registry object found. Expected registry object to be of type '"
            + MetricRegistry.class.getName()
            + "'"
      }
    };
  }
}
