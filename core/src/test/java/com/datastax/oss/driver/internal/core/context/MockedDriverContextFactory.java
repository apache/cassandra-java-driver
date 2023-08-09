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
package com.datastax.oss.driver.internal.core.context;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import java.time.Duration;
import java.util.Optional;

public class MockedDriverContextFactory {

  public static DefaultDriverContext defaultDriverContext() {
    return defaultDriverContext(Optional.empty());
  }

  public static DefaultDriverContext defaultDriverContext(
      Optional<DriverExecutionProfile> profileOption) {

    /* If the caller provided a profile use that, otherwise make a new one */
    final DriverExecutionProfile profile =
        profileOption.orElseGet(
            () -> {
              DriverExecutionProfile blankProfile = mock(DriverExecutionProfile.class);
              when(blankProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
                  .thenReturn("none");
              when(blankProfile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
                  .thenReturn(Duration.ofMinutes(5));
              when(blankProfile.isDefined(DefaultDriverOption.METRICS_FACTORY_CLASS))
                  .thenReturn(true);
              when(blankProfile.getString(DefaultDriverOption.METRICS_FACTORY_CLASS))
                  .thenReturn("DefaultMetricsFactory");
              return blankProfile;
            });

    /* Setup machinery to connect the input DriverExecutionProfile to the config loader */
    final DriverConfig driverConfig = mock(DriverConfig.class);
    final DriverConfigLoader configLoader = mock(DriverConfigLoader.class);
    when(configLoader.getInitialConfig()).thenReturn(driverConfig);
    when(driverConfig.getDefaultProfile()).thenReturn(profile);

    ProgrammaticArguments args =
        ProgrammaticArguments.builder()
            .withNodeStateListener(mock(NodeStateListener.class))
            .withSchemaChangeListener(mock(SchemaChangeListener.class))
            .withRequestTracker(mock(RequestTracker.class))
            .withLocalDatacenters(Maps.newHashMap())
            .withNodeDistanceEvaluators(Maps.newHashMap())
            .build();
    return new DefaultDriverContext(configLoader, args);
  }
}
