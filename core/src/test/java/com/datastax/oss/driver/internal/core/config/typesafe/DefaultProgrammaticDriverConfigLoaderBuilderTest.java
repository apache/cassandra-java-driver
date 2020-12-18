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
package com.datastax.oss.driver.internal.core.config.typesafe;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.MockOptions;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class DefaultProgrammaticDriverConfigLoaderBuilderTest {

  private static final String FALLBACK_CONFIG =
      "int1 = 1\nint2 = 2\nprofiles.profile1 { int1 = 11 }";

  @Test
  public void should_override_option() {
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder(
                () -> ConfigFactory.parseString(FALLBACK_CONFIG), "")
            .withInt(MockOptions.INT1, 2)
            .withInt(MockOptions.INT1, 3)
            .withInt(MockOptions.INT1, 4)
            .withInt(MockOptions.INT2, 3)
            .withInt(MockOptions.INT2, 4)
            .build();
    DriverConfig config = loader.getInitialConfig();
    assertThat(config.getDefaultProfile().getInt(MockOptions.INT1)).isEqualTo(4);
    assertThat(config.getDefaultProfile().getInt(MockOptions.INT2)).isEqualTo(4);
  }

  @Test
  public void should_override_option_in_default_profile() {
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder(
                () -> ConfigFactory.parseString(FALLBACK_CONFIG), "")
            .withInt(MockOptions.INT1, 3)
            .build();
    DriverConfig config = loader.getInitialConfig();
    assertThat(config.getDefaultProfile().getInt(MockOptions.INT1)).isEqualTo(3);
    assertThat(config.getDefaultProfile().getInt(MockOptions.INT2)).isEqualTo(2);
  }

  @Test
  public void should_override_option_in_existing_profile() {
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder(
                () -> ConfigFactory.parseString(FALLBACK_CONFIG), "")
            .startProfile("profile1")
            .withInt(MockOptions.INT1, 3)
            .build();
    DriverConfig config = loader.getInitialConfig();
    assertThat(config.getDefaultProfile().getInt(MockOptions.INT1)).isEqualTo(1);
    assertThat(config.getProfile("profile1").getInt(MockOptions.INT1)).isEqualTo(3);
  }

  @Test
  public void should_override_option_in_new_profile() {
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder(
                () -> ConfigFactory.parseString(FALLBACK_CONFIG), "")
            .startProfile("profile2")
            .withInt(MockOptions.INT1, 3)
            .build();
    DriverConfig config = loader.getInitialConfig();
    assertThat(config.getDefaultProfile().getInt(MockOptions.INT1)).isEqualTo(1);
    assertThat(config.getProfile("profile1").getInt(MockOptions.INT1)).isEqualTo(11);
    assertThat(config.getProfile("profile2").getInt(MockOptions.INT1)).isEqualTo(3);
    assertThat(config.getProfile("profile2").getInt(MockOptions.INT2)).isEqualTo(2);
  }

  @Test
  public void should_go_back_to_default_profile_when_profile_ends() {
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder(
                () -> ConfigFactory.parseString(FALLBACK_CONFIG), "")
            .startProfile("profile2")
            .withInt(MockOptions.INT1, 3)
            .endProfile()
            .withInt(MockOptions.INT1, 4)
            .build();
    DriverConfig config = loader.getInitialConfig();
    assertThat(config.getDefaultProfile().getInt(MockOptions.INT1)).isEqualTo(4);
  }

  @Test
  public void should_handle_multiple_programmatic_profiles() {
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder(
                () -> ConfigFactory.parseString(FALLBACK_CONFIG), "")
            .startProfile("profile2")
            .withInt(MockOptions.INT1, 3)
            .startProfile("profile3")
            .withInt(MockOptions.INT1, 4)
            .build();
    DriverConfig config = loader.getInitialConfig();
    assertThat(config.getProfile("profile2").getInt(MockOptions.INT1)).isEqualTo(3);
    assertThat(config.getProfile("profile3").getInt(MockOptions.INT1)).isEqualTo(4);
  }

  @Test
  public void should_honor_root_path() {
    String rootPath = "test-root";
    String propertyKey = rootPath + "." + DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE.getPath();
    try {
      System.setProperty(propertyKey, "42");
      DriverConfigLoader loader =
          new DefaultProgrammaticDriverConfigLoaderBuilder(
                  DefaultProgrammaticDriverConfigLoaderBuilder.DEFAULT_FALLBACK_SUPPLIER, rootPath)
              .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 1234)
              .build();
      DriverConfig config = loader.getInitialConfig();
      assertThat(config.getDefaultProfile().getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
          .isEqualTo(42);
      assertThat(config.getDefaultProfile().getInt(DefaultDriverOption.REQUEST_PAGE_SIZE))
          .isEqualTo(1234);
    } finally {
      System.clearProperty(propertyKey);
    }
  }
}
