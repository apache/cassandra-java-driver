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
package com.datastax.oss.driver.internal.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverOption;
import org.assertj.core.api.AbstractAssert;

public class DriverConfigAssert extends AbstractAssert<DriverConfigAssert, DriverConfig> {
  public DriverConfigAssert(DriverConfig actual) {
    super(actual, DriverConfigAssert.class);
  }

  public DriverConfigAssert hasIntOption(DriverOption option, int expected) {
    assertThat(actual.getDefaultProfile().getInt(option)).isEqualTo(expected);
    return this;
  }

  public DriverConfigAssert hasIntOption(String profileName, DriverOption option, int expected) {
    assertThat(actual.getProfile(profileName).getInt(option)).isEqualTo(expected);
    return this;
  }
}
