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
package com.datastax.oss.driver.internal.core.config.map;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.internal.core.config.MockOptions;
import com.datastax.oss.driver.internal.core.config.MockTypedOptions;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import java.util.Map;
import java.util.SortedSet;
import org.junit.Test;

public class MapBasedDriverConfigLoaderTest {

  @Test
  public void should_reflect_changes_in_real_time() {
    OptionsMap source = new OptionsMap();
    source.put(MockTypedOptions.INT1, 1);

    DriverConfigLoader loader = DriverConfigLoader.fromMap(source);
    DriverConfig config = loader.getInitialConfig();
    assertThat(config.getDefaultProfile().getInt(MockOptions.INT1)).isEqualTo(1);

    source.put(MockTypedOptions.INT1, 2);
    assertThat(config.getDefaultProfile().getInt(MockOptions.INT1)).isEqualTo(2);
  }

  /**
   * Checks that, if we ask to pre-fill the default profile, then we get the same set of options as
   * the built-in reference.conf.
   */
  @Test
  public void should_fill_default_profile_like_reference_file() {
    SortedSet<Map.Entry<String, Object>> memoryBased =
        DriverConfigLoader.fromMap(OptionsMap.driverDefaults())
            .getInitialConfig()
            .getDefaultProfile()
            .entrySet();
    SortedSet<Map.Entry<String, Object>> fileBased =
        new DefaultDriverConfigLoader().getInitialConfig().getDefaultProfile().entrySet();

    for (Map.Entry<String, Object> entry : fileBased) {
      if (entry.getKey().equals(DefaultDriverOption.CONFIG_RELOAD_INTERVAL.getPath())) {
        continue;
      }
      assertThat(memoryBased).as("Missing entry: " + entry).contains(entry);
    }
    assertThat(memoryBased).hasSize(fileBased.size() - 1);
  }
}
