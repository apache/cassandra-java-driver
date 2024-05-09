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
package com.datastax.oss.driver.internal.core.config.map;

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.internal.core.config.MockOptions;
import com.datastax.oss.driver.internal.core.config.MockTypedOptions;
import org.junit.Test;

public class MapBasedDriverConfigTest {

  @Test
  public void should_load_minimal_config_with_no_profiles() {
    OptionsMap source = new OptionsMap();
    source.put(MockTypedOptions.INT1, 42);
    DriverConfig config = DriverConfigLoader.fromMap(source).getInitialConfig();

    assertThat(config).hasIntOption(MockOptions.INT1, 42);
  }

  @Test
  public void should_inherit_option_in_profile() {
    OptionsMap source = new OptionsMap();
    source.put(MockTypedOptions.INT1, 42);
    // need to add an unrelated option to create the profile
    source.put("profile1", MockTypedOptions.INT2, 1);
    DriverConfig config = DriverConfigLoader.fromMap(source).getInitialConfig();

    assertThat(config)
        .hasIntOption(MockOptions.INT1, 42)
        .hasIntOption("profile1", MockOptions.INT1, 42);
  }

  @Test
  public void should_override_option_in_profile() {
    OptionsMap source = new OptionsMap();
    source.put(MockTypedOptions.INT1, 42);
    source.put("profile1", MockTypedOptions.INT1, 43);
    DriverConfig config = DriverConfigLoader.fromMap(source).getInitialConfig();

    assertThat(config)
        .hasIntOption(MockOptions.INT1, 42)
        .hasIntOption("profile1", MockOptions.INT1, 43);
  }

  @Test
  public void should_create_derived_profile_with_new_option() {
    OptionsMap source = new OptionsMap();
    source.put(MockTypedOptions.INT1, 42);
    DriverConfig config = DriverConfigLoader.fromMap(source).getInitialConfig();
    DriverExecutionProfile base = config.getDefaultProfile();
    DriverExecutionProfile derived = base.withInt(MockOptions.INT2, 43);

    assertThat(base.isDefined(MockOptions.INT2)).isFalse();
    assertThat(derived.isDefined(MockOptions.INT2)).isTrue();
    assertThat(derived.getInt(MockOptions.INT2)).isEqualTo(43);
  }

  @Test
  public void should_create_derived_profile_overriding_option() {
    OptionsMap source = new OptionsMap();
    source.put(MockTypedOptions.INT1, 42);
    DriverConfig config = DriverConfigLoader.fromMap(source).getInitialConfig();
    DriverExecutionProfile base = config.getDefaultProfile();
    DriverExecutionProfile derived = base.withInt(MockOptions.INT1, 43);

    assertThat(base.getInt(MockOptions.INT1)).isEqualTo(42);
    assertThat(derived.getInt(MockOptions.INT1)).isEqualTo(43);
  }

  @Test
  public void should_create_derived_profile_unsetting_option() {
    OptionsMap source = new OptionsMap();
    source.put(MockTypedOptions.INT1, 42);
    source.put(MockTypedOptions.INT2, 43);
    DriverConfig config = DriverConfigLoader.fromMap(source).getInitialConfig();
    DriverExecutionProfile base = config.getDefaultProfile();
    DriverExecutionProfile derived = base.without(MockOptions.INT2);

    assertThat(base.getInt(MockOptions.INT2)).isEqualTo(43);
    assertThat(derived.isDefined(MockOptions.INT2)).isFalse();
  }
}
