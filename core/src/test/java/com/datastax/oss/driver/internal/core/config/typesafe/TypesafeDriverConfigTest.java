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

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.config.MockOptions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class TypesafeDriverConfigTest {

  @Test
  public void should_load_minimal_config_with_no_profiles() {
    TypesafeDriverConfig config = parse("int1 = 42");
    assertThat(config).hasIntOption(MockOptions.INT1, 42);
  }

  @Test
  public void should_load_config_with_no_profiles_and_optional_values() {
    TypesafeDriverConfig config = parse("int1 = 42\n int2 = 43");
    assertThat(config).hasIntOption(MockOptions.INT1, 42);
    assertThat(config).hasIntOption(MockOptions.INT2, 43);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_if_profile_uses_default_name() {
    parse("int1 = 42\n profiles { default { int1 = 43 } }");
  }

  @Test
  public void should_inherit_option_in_profile() {
    TypesafeDriverConfig config = parse("int1 = 42\n profiles { profile1 { } }");
    assertThat(config)
        .hasIntOption(MockOptions.INT1, 42)
        .hasIntOption("profile1", MockOptions.INT1, 42);
  }

  @Test
  public void should_override_option_in_profile() {
    TypesafeDriverConfig config = parse("int1 = 42\n profiles { profile1 { int1 = 43 } }");
    assertThat(config)
        .hasIntOption(MockOptions.INT1, 42)
        .hasIntOption("profile1", MockOptions.INT1, 43);
  }

  @Test
  public void should_create_derived_profile_with_new_option() {
    TypesafeDriverConfig config = parse("int1 = 42");
    DriverExecutionProfile base = config.getDefaultProfile();
    DriverExecutionProfile derived = base.withInt(MockOptions.INT2, 43);

    assertThat(base.isDefined(MockOptions.INT2)).isFalse();
    assertThat(derived.isDefined(MockOptions.INT2)).isTrue();
    assertThat(derived.getInt(MockOptions.INT2)).isEqualTo(43);
  }

  @Test
  public void should_create_derived_profile_overriding_option() {
    TypesafeDriverConfig config = parse("int1 = 42");
    DriverExecutionProfile base = config.getDefaultProfile();
    DriverExecutionProfile derived = base.withInt(MockOptions.INT1, 43);

    assertThat(base.getInt(MockOptions.INT1)).isEqualTo(42);
    assertThat(derived.getInt(MockOptions.INT1)).isEqualTo(43);
  }

  @Test
  public void should_create_derived_profile_unsetting_option() {
    TypesafeDriverConfig config = parse("int1 = 42\n int2 = 43");
    DriverExecutionProfile base = config.getDefaultProfile();
    DriverExecutionProfile derived = base.without(MockOptions.INT2);

    assertThat(base.getInt(MockOptions.INT2)).isEqualTo(43);
    assertThat(derived.isDefined(MockOptions.INT2)).isFalse();
  }

  @Test
  public void should_fetch_string_map() {
    TypesafeDriverConfig config =
        parse(
            "int1 = 42 \n auth_provider { auth_thing_one= one \n auth_thing_two = two \n auth_thing_three = three}");
    DriverExecutionProfile base = config.getDefaultProfile();
    base.getStringMap(MockOptions.AUTH_PROVIDER);
    Map<String, String> map = base.getStringMap(MockOptions.AUTH_PROVIDER);
    assertThat(map.entrySet().size()).isEqualTo(3);
    assertThat(map.get("auth_thing_one")).isEqualTo("one");
    assertThat(map.get("auth_thing_two")).isEqualTo("two");
    assertThat(map.get("auth_thing_three")).isEqualTo("three");
  }

  @Test
  public void should_create_derived_profile_with_string_map() {
    TypesafeDriverConfig config = parse("int1 = 42");
    Map<String, String> authThingMap = new HashMap<>();
    authThingMap.put("auth_thing_one", "one");
    authThingMap.put("auth_thing_two", "two");
    authThingMap.put("auth_thing_three", "three");
    DriverExecutionProfile base = config.getDefaultProfile();
    DriverExecutionProfile mapBase = base.withStringMap(MockOptions.AUTH_PROVIDER, authThingMap);
    Map<String, String> fetchedMap = mapBase.getStringMap(MockOptions.AUTH_PROVIDER);
    assertThat(fetchedMap).isEqualTo(authThingMap);
  }

  @Test
  public void should_reload() {
    TypesafeDriverConfig config = parse("int1 = 42\n profiles { profile1 { int1 = 43 } }");

    config.reload(ConfigFactory.parseString("int1 = 44\n profiles { profile1 { int1 = 45 } }"));
    assertThat(config)
        .hasIntOption(MockOptions.INT1, 44)
        .hasIntOption("profile1", MockOptions.INT1, 45);
  }

  @Test
  public void should_update_derived_profiles_after_reloading() {
    TypesafeDriverConfig config = parse("int1 = 42\n profiles { profile1 { int1 = 43 } }");

    DriverExecutionProfile derivedFromDefault =
        config.getDefaultProfile().withInt(MockOptions.INT2, 50);
    DriverExecutionProfile derivedFromProfile1 =
        config.getProfile("profile1").withInt(MockOptions.INT2, 51);

    config.reload(ConfigFactory.parseString("int1 = 44\n profiles { profile1 { int1 = 45 } }"));

    assertThat(derivedFromDefault.getInt(MockOptions.INT1)).isEqualTo(44);
    assertThat(derivedFromDefault.getInt(MockOptions.INT2)).isEqualTo(50);

    assertThat(derivedFromProfile1.getInt(MockOptions.INT1)).isEqualTo(45);
    assertThat(derivedFromProfile1.getInt(MockOptions.INT2)).isEqualTo(51);
  }

  @Test
  public void should_enumerate_options() {
    TypesafeDriverConfig config =
        parse(
            "int1 = 42 \n"
                + "auth_provider { auth_thing_one= one \n auth_thing_two = two \n auth_thing_three = three}\n"
                + "profiles { profile1 { int1 = 45 } }");

    assertThat(config.getDefaultProfile().entrySet())
        .containsExactly(
            entry("auth_provider.auth_thing_one", "one"),
            entry("auth_provider.auth_thing_three", "three"),
            entry("auth_provider.auth_thing_two", "two"),
            entry("int1", 42));

    assertThat(config.getProfile("profile1").entrySet())
        .containsExactly(
            entry("auth_provider.auth_thing_one", "one"),
            entry("auth_provider.auth_thing_three", "three"),
            entry("auth_provider.auth_thing_two", "two"),
            entry("int1", 45));
  }

  private TypesafeDriverConfig parse(String configString) {
    Config config = ConfigFactory.parseString(configString);
    return new TypesafeDriverConfig(config);
  }
}
