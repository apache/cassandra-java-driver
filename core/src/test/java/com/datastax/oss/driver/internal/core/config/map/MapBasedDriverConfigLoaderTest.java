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

import static com.typesafe.config.ConfigFactory.defaultReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.config.MockOptions;
import com.datastax.oss.driver.internal.core.config.MockTypedOptions;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.util.Optional;
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
    OptionsMap optionsMap = OptionsMap.driverDefaults();
    DriverExecutionProfile mapBasedConfig =
        DriverConfigLoader.fromMap(optionsMap).getInitialConfig().getDefaultProfile();
    DriverExecutionProfile fileBasedConfig =
        new DefaultDriverConfigLoader(
                () -> {
                  // Only load reference.conf since we are focusing on driver defaults
                  ConfigFactory.invalidateCaches();
                  return defaultReference().getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
                })
            .getInitialConfig()
            .getDefaultProfile();

    // Make sure we're not missing any options. -1 is for CONFIG_RELOAD_INTERVAL, which is not
    // defined by OptionsMap because it is irrelevant for the map-based config.
    assertThat(mapBasedConfig.entrySet()).hasSize(fileBasedConfig.entrySet().size() - 1);

    for (TypedDriverOption<?> option : TypedDriverOption.builtInValues()) {
      if (option.getRawOption() == DefaultDriverOption.CONFIG_RELOAD_INTERVAL) {
        continue;
      }
      Optional<Object> fileBasedValue = get(fileBasedConfig, option);
      Optional<Object> mapBasedValue = get(mapBasedConfig, option);
      assertThat(mapBasedValue)
          .as("Wrong value for %s in OptionsMap", option.getRawOption())
          .isEqualTo(fileBasedValue);
    }
  }

  private Optional<Object> get(DriverExecutionProfile config, TypedDriverOption<?> typedOption) {
    DriverOption option = typedOption.getRawOption();
    GenericType<?> type = typedOption.getExpectedType();
    Object value = null;
    if (config.isDefined(option)) {
      // This is ugly, we have no other way than enumerating all possible types.
      // This kind of bridging code between OptionsMap and DriverConfig is unlikely to exist
      // anywhere outside of this test.
      if (type.equals(GenericType.listOf(String.class))) {
        value = config.getStringList(option);
      } else if (type.equals(GenericType.STRING)) {
        value = config.getString(option);
      } else if (type.equals(GenericType.DURATION)) {
        value = config.getDuration(option);
      } else if (type.equals(GenericType.INTEGER)) {
        value = config.getInt(option);
      } else if (type.equals(GenericType.BOOLEAN)) {
        value = config.getBoolean(option);
      } else if (type.equals(GenericType.LONG)) {
        try {
          value = config.getLong(option);
        } catch (ConfigException.WrongType e) {
          value = config.getBytes(option);
        }
      } else if (type.equals(GenericType.mapOf(GenericType.STRING, GenericType.STRING))) {
        value = config.getStringMap(option);
      } else {
        fail("Unexpected type " + type);
      }
    }
    return Optional.ofNullable(value);
  }
}
