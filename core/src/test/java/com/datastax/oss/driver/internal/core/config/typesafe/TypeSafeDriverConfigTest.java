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
package com.datastax.oss.driver.internal.core.config.typesafe;

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class TypeSafeDriverConfigTest {

  @Test
  public void should_load_minimal_config_with_required_options_and_no_profiles() {
    DriverConfig config = parse("required_int = 42");
    assertThat(config).hasIntOption(MockOptions.REQUIRED_INT, 42);
  }

  @Test
  public void should_load_config_with_no_profiles_and_optional_values() {
    DriverConfig config = parse("required_int = 42\n optional_int = 43");
    assertThat(config).hasIntOption(MockOptions.REQUIRED_INT, 42);
    assertThat(config).hasIntOption(MockOptions.OPTIONAL_INT, 43);
  }

  @Test(
    expectedExceptions = IllegalArgumentException.class,
    expectedExceptionsMessageRegExp = "Missing option required_int. Check your configuration file."
  )
  public void should_fail_if_required_option_is_missing() {
    parse("");
  }

  @Test
  public void should_inherit_option_in_profile() {
    DriverConfig config = parse("required_int = 42\n profiles { profile1 { } }");
    assertThat(config)
        .hasIntOption(MockOptions.REQUIRED_INT, 42)
        .hasIntOption("profile1", MockOptions.REQUIRED_INT, 42);
  }

  @Test
  public void should_override_option_in_profile() {
    DriverConfig config = parse("required_int = 42\n profiles { profile1 { required_int = 43 } }");
    assertThat(config)
        .hasIntOption(MockOptions.REQUIRED_INT, 42)
        .hasIntOption("profile1", MockOptions.REQUIRED_INT, 43);
  }

  @Test
  public void should_load_default_driver_config() {
    // No assertions here, but this validates that `reference.conf` is well-formed.
    new TypeSafeDriverConfig(
        ConfigFactory.load().getConfig("datastax-java-driver"), CoreDriverOption.values());
  }

  private DriverConfig parse(String configString) {
    Config config = ConfigFactory.parseString(configString);
    return new TypeSafeDriverConfig(config, MockOptions.values());
  }
}
