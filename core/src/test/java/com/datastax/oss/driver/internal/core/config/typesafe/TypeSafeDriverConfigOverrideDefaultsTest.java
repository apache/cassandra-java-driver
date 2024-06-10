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
package com.datastax.oss.driver.internal.core.config.typesafe;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.Map;
import org.junit.Test;

/** Focuses on {@link TypesafeDriverConfig#overrideDefaults(Map)}. */
public class TypeSafeDriverConfigOverrideDefaultsTest {

  @Test
  public void should_replace_if_value_comes_from_reference() {
    // Given
    TypesafeDriverConfig config = config("");
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_ONE");

    // When
    config.overrideDefaults(
        ImmutableMap.of(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM"));

    // Then
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_QUORUM");
  }

  @Test
  public void should_replace_multiple_times() {
    // Given
    TypesafeDriverConfig config = config("");
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_ONE");

    // When
    config.overrideDefaults(
        ImmutableMap.of(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM"));
    config.overrideDefaults(ImmutableMap.of(DefaultDriverOption.REQUEST_CONSISTENCY, "TWO"));

    // Then
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("TWO");
  }

  @Test
  public void should_not_replace_if_overridden_from_application() {
    // Given
    TypesafeDriverConfig config =
        config("datastax-java-driver.basic.request.consistency = LOCAL_ONE");
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_ONE");

    // When
    config.overrideDefaults(
        ImmutableMap.of(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM"));

    // Then
    // not replaced because it was set explictly in application.conf
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_ONE");
  }

  @Test
  public void should_handle_reloads() {
    // Given
    TypesafeDriverConfig config = config("");
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_ONE");

    // When
    config.overrideDefaults(
        ImmutableMap.of(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM"));
    reload(config, "");

    // Then
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_QUORUM");

    // When
    reload(config, "datastax-java-driver.basic.request.consistency = ONE");

    // Then
    // overridden default not used anymore if the reload detected a user change
    assertThat(config.getDefaultProfile().getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("ONE");
  }

  @Test
  public void should_ignore_non_existent_option() {
    // Given
    TypesafeDriverConfig config = config("");
    DriverOption nonExistent = () -> "non existent";

    // When
    config.overrideDefaults(ImmutableMap.of(nonExistent, "IRRELEVANT"));

    // Then
    assertThat(config.getDefaultProfile().isDefined(nonExistent)).isFalse();
  }

  @Test
  public void should_handle_profiles() {
    // Given
    TypesafeDriverConfig config =
        config(
            "datastax-java-driver.profiles.profile1.basic.request.consistency = TWO\n"
                + "datastax-java-driver.profiles.profile2.basic.request.timeout = 5 seconds");
    DriverExecutionProfile profile1 = config.getProfile("profile1");
    DriverExecutionProfile profile2 = config.getProfile("profile2");
    DriverExecutionProfile derivedProfile21 =
        profile2.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10));
    DriverExecutionProfile derivedProfile22 =
        profile2.withString(DefaultDriverOption.REQUEST_CONSISTENCY, "QUORUM");
    assertThat(profile1.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).isEqualTo("TWO");
    assertThat(profile2.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_ONE"); // inherited from default profile in reference.conf
    assertThat(derivedProfile21.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_ONE"); // inherited from default profile in reference.conf
    assertThat(derivedProfile22.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("QUORUM"); // overridden programmatically

    // When
    config.overrideDefaults(
        ImmutableMap.of(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM"));

    // Then
    // Unaffected because it was set manually in application.conf:
    assertThat(profile1.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).isEqualTo("TWO");
    // Affected because it was using the default from reference.conf:
    assertThat(profile2.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_QUORUM");
    // Same:
    assertThat(derivedProfile21.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("LOCAL_QUORUM");
    // Unaffected because it was overridden programmatically:
    assertThat(derivedProfile22.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo("QUORUM");
  }

  // Builds a config based on reference.conf + the given application.conf overrides
  private TypesafeDriverConfig config(String application) {
    return new TypesafeDriverConfig(rawConfig(application));
  }

  private boolean reload(TypesafeDriverConfig config, String newApplication) {
    return config.reload(rawConfig(newApplication));
  }

  private Config rawConfig(String application) {
    ConfigFactory.invalidateCaches();
    return ConfigFactory.parseString(application)
        .withFallback(ConfigFactory.defaultReference())
        .resolve()
        .getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
  }
}
