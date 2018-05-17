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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TypesafeDriverConfigTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void should_load_minimal_config_with_required_options_and_no_profiles() {
    TypesafeDriverConfig config = parse("required_int = 42");
    assertThat(config).hasIntOption(MockOptions.REQUIRED_INT, 42);
  }

  @Test
  public void should_load_config_with_no_profiles_and_optional_values() {
    TypesafeDriverConfig config = parse("required_int = 42\n optional_int = 43");
    assertThat(config).hasIntOption(MockOptions.REQUIRED_INT, 42);
    assertThat(config).hasIntOption(MockOptions.OPTIONAL_INT, 43);
  }

  @Test
  public void should_fail_if_required_option_is_missing() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Missing option required_int. Check your configuration file.");
    parse("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_if_profile_uses_default_name() {
    parse("required_int = 42\n profiles { default { required_int = 43 } }");
  }

  @Test
  public void should_inherit_option_in_profile() {
    TypesafeDriverConfig config = parse("required_int = 42\n profiles { profile1 { } }");
    assertThat(config)
        .hasIntOption(MockOptions.REQUIRED_INT, 42)
        .hasIntOption("profile1", MockOptions.REQUIRED_INT, 42);
  }

  @Test
  public void should_override_option_in_profile() {
    TypesafeDriverConfig config =
        parse("required_int = 42\n profiles { profile1 { required_int = 43 } }");
    assertThat(config)
        .hasIntOption(MockOptions.REQUIRED_INT, 42)
        .hasIntOption("profile1", MockOptions.REQUIRED_INT, 43);
  }

  @Test
  public void should_load_default_driver_config() {
    // No assertions here, but this validates that `reference.conf` is well-formed.
    new TypesafeDriverConfig(
        ConfigFactory.load().getConfig("datastax-java-driver"), DefaultDriverOption.values());
  }

  @Test
  public void should_create_derived_profile_with_new_option() {
    TypesafeDriverConfig config = parse("required_int = 42");
    DriverConfigProfile base = config.getDefaultProfile();
    DriverConfigProfile derived = base.withInt(MockOptions.OPTIONAL_INT, 43);

    assertThat(base.isDefined(MockOptions.OPTIONAL_INT)).isFalse();
    assertThat(derived.isDefined(MockOptions.OPTIONAL_INT)).isTrue();
    assertThat(derived.getInt(MockOptions.OPTIONAL_INT)).isEqualTo(43);
  }

  @Test
  public void should_create_derived_profile_overriding_option() {
    TypesafeDriverConfig config = parse("required_int = 42");
    DriverConfigProfile base = config.getDefaultProfile();
    DriverConfigProfile derived = base.withInt(MockOptions.REQUIRED_INT, 43);

    assertThat(base.getInt(MockOptions.REQUIRED_INT)).isEqualTo(42);
    assertThat(derived.getInt(MockOptions.REQUIRED_INT)).isEqualTo(43);
  }

  @Test
  public void should_fetch_string_map() {
    TypesafeDriverConfig config =
        parse(
            "required_int = 42 \n auth_provider { auth_thing_one= one \n auth_thing_two = two \n auth_thing_three = three}");
    DriverConfigProfile base = config.getDefaultProfile();
    base.getStringMap(MockOptions.OPTIONAL_AUTH);
    Map<String, String> map = base.getStringMap(MockOptions.OPTIONAL_AUTH);
    assertThat(map.entrySet().size()).isEqualTo(3);
    assertThat(map.get("auth_thing_one")).isEqualTo("one");
    assertThat(map.get("auth_thing_two")).isEqualTo("two");
    assertThat(map.get("auth_thing_three")).isEqualTo("three");
  }

  @Test
  public void should_create_derived_profile_with_string_map() {
    TypesafeDriverConfig config = parse("required_int = 42");
    Map<String, String> authThingMap = new HashMap<>();
    authThingMap.put("auth_thing_one", "one");
    authThingMap.put("auth_thing_two", "two");
    authThingMap.put("auth_thing_three", "three");
    DriverConfigProfile base = config.getDefaultProfile();
    DriverConfigProfile mapBase = base.withStringMap(MockOptions.OPTIONAL_AUTH, authThingMap);
    Map<String, String> fetchedMap = mapBase.getStringMap(MockOptions.OPTIONAL_AUTH);
    assertThat(fetchedMap).isEqualTo(authThingMap);
  }

  @Test
  public void should_reload() {
    TypesafeDriverConfig config =
        parse("required_int = 42\n profiles { profile1 { required_int = 43 } }");

    config.reload(
        ConfigFactory.parseString(
            "required_int = 44\n profiles { profile1 { required_int = 45 } }"));
    assertThat(config)
        .hasIntOption(MockOptions.REQUIRED_INT, 44)
        .hasIntOption("profile1", MockOptions.REQUIRED_INT, 45);
  }

  @Test
  public void should_update_derived_profiles_after_reloading() {
    TypesafeDriverConfig config =
        parse("required_int = 42\n profiles { profile1 { required_int = 43 } }");

    DriverConfigProfile derivedFromDefault =
        config.getDefaultProfile().withInt(MockOptions.OPTIONAL_INT, 50);
    DriverConfigProfile derivedFromProfile1 =
        config.getProfile("profile1").withInt(MockOptions.OPTIONAL_INT, 51);

    config.reload(
        ConfigFactory.parseString(
            "required_int = 44\n profiles { profile1 { required_int = 45 } }"));

    assertThat(derivedFromDefault.getInt(MockOptions.REQUIRED_INT)).isEqualTo(44);
    assertThat(derivedFromDefault.getInt(MockOptions.OPTIONAL_INT)).isEqualTo(50);

    assertThat(derivedFromProfile1.getInt(MockOptions.REQUIRED_INT)).isEqualTo(45);
    assertThat(derivedFromProfile1.getInt(MockOptions.OPTIONAL_INT)).isEqualTo(51);
  }

  @Test
  public void should_enumerate_options() {
    TypesafeDriverConfig config =
        parse(
            "required_int = 42 \n"
                + "auth_provider { auth_thing_one= one \n auth_thing_two = two \n auth_thing_three = three}\n"
                + "profiles { profile1 { required_int = 45 } }");

    assertThat(config.getDefaultProfile().entrySet())
        .containsExactly(
            entry("auth_provider.auth_thing_one", "one"),
            entry("auth_provider.auth_thing_three", "three"),
            entry("auth_provider.auth_thing_two", "two"),
            entry("required_int", 42));

    assertThat(config.getProfile("profile1").entrySet())
        .containsExactly(
            entry("auth_provider.auth_thing_one", "one"),
            entry("auth_provider.auth_thing_three", "three"),
            entry("auth_provider.auth_thing_two", "two"),
            entry("required_int", 45));
  }

  private TypesafeDriverConfig parse(String configString) {
    Config config = ConfigFactory.parseString(configString);
    return new TypesafeDriverConfig(config, MockOptions.values());
  }
}
