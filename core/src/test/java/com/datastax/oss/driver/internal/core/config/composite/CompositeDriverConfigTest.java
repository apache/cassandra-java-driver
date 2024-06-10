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
package com.datastax.oss.driver.internal.core.config.composite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import org.junit.Before;
import org.junit.Test;

public class CompositeDriverConfigTest {

  private OptionsMap primaryMap;
  private OptionsMap fallbackMap;
  private DriverConfig compositeConfig;
  private DriverExecutionProfile compositeDefaultProfile;

  @Before
  public void setup() {
    primaryMap = new OptionsMap();
    // We need at least one option so that the default profile exists. Do it now to avoid having to
    // do it in every test. We use an option that we won't reuse in the tests so that there are no
    // unwanted interactions.
    primaryMap.put(TypedDriverOption.CONTINUOUS_PAGING_MAX_PAGES, 1);

    fallbackMap = new OptionsMap();
    fallbackMap.put(TypedDriverOption.CONTINUOUS_PAGING_MAX_PAGES, 1);

    DriverConfigLoader compositeLoader =
        DriverConfigLoader.compose(
            DriverConfigLoader.fromMap(primaryMap), DriverConfigLoader.fromMap(fallbackMap));
    compositeConfig = compositeLoader.getInitialConfig();
    compositeDefaultProfile = compositeConfig.getDefaultProfile();
  }

  @Test
  public void should_use_value_from_primary_config() {
    primaryMap.put(TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1);

    assertThat(compositeDefaultProfile.isDefined(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
        .isTrue();
    assertThat(compositeDefaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
        .isEqualTo(1);
    assertThat(compositeDefaultProfile.entrySet())
        .containsExactly(
            entry(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE.getPath(), 1),
            entry(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES.getPath(), 1));
  }

  @Test
  public void should_ignore_value_from_fallback_config_if_defined_in_both() {
    primaryMap.put(TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1);
    fallbackMap.put(TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 2);

    assertThat(compositeDefaultProfile.isDefined(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
        .isTrue();
    assertThat(compositeDefaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
        .isEqualTo(1);
    assertThat(compositeDefaultProfile.entrySet())
        .containsExactly(
            entry(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE.getPath(), 1),
            entry(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES.getPath(), 1));
  }

  @Test
  public void should_use_value_from_fallback_config_if_not_defined_in_primary() {
    fallbackMap.put(TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1);

    assertThat(compositeDefaultProfile.isDefined(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
        .isTrue();
    assertThat(compositeDefaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
        .isEqualTo(1);
    assertThat(compositeDefaultProfile.entrySet())
        .containsExactly(
            entry(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE.getPath(), 1),
            entry(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES.getPath(), 1));
  }

  @Test
  public void should_merge_profiles() {
    primaryMap.put("onlyInPrimary", TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1);
    primaryMap.put("inBoth", TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 2);
    fallbackMap.put("inBoth", TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 3);
    fallbackMap.put("onlyInFallback", TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4);

    assertThat(compositeConfig.getProfiles())
        .containsKeys(
            DriverExecutionProfile.DEFAULT_NAME,
            "onlyInPrimary",
            "inBoth",
            "inBoth",
            "onlyInFallback");

    assertThat(
            compositeConfig
                .getProfile("onlyInPrimary")
                .getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
        .isEqualTo(1);
    assertThat(
            compositeConfig
                .getProfile("inBoth")
                .getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
        .isEqualTo(2);
    assertThat(
            compositeConfig
                .getProfile("onlyInFallback")
                .getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE))
        .isEqualTo(4);

    assertThat(compositeConfig.getProfile("onlyInPrimary").entrySet())
        .containsExactly(
            entry(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE.getPath(), 1),
            entry(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES.getPath(), 1));

    assertThat(compositeConfig.getProfile("inBoth").entrySet())
        .containsExactly(
            entry(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE.getPath(), 2),
            entry(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES.getPath(), 1));

    assertThat(compositeConfig.getProfile("onlyInFallback").entrySet())
        .containsExactly(
            entry(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE.getPath(), 4),
            entry(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES.getPath(), 1));
  }
}
