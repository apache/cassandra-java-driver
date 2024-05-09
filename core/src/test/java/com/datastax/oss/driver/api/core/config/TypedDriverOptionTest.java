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
package com.datastax.oss.driver.api.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class TypedDriverOptionTest {

  /**
   * Checks that every built-in {@link DriverOption} has an equivalent constant in {@link
   * TypedDriverOption}.
   */
  @Test
  public void should_have_equivalents_for_all_builtin_untyped_options() {
    Set<DriverOption> optionsThatHaveATypedEquivalent = new HashSet<>();
    for (TypedDriverOption<?> typedOption : TypedDriverOption.builtInValues()) {
      optionsThatHaveATypedEquivalent.add(typedOption.getRawOption());
    }

    // These options are only used internally to compare policy configurations across profiles.
    // Users never use them directly, so they don't need typed equivalents.
    Set<DriverOption> exclusions =
        ImmutableSet.of(
            DefaultDriverOption.LOAD_BALANCING_POLICY,
            DefaultDriverOption.RETRY_POLICY,
            DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY);

    for (DriverOption option :
        ImmutableSet.<DriverOption>builder()
            .add(DefaultDriverOption.values())
            .add(DseDriverOption.values())
            .build()) {
      if (!exclusions.contains(option)) {
        assertThat(optionsThatHaveATypedEquivalent)
            .as(
                "Couldn't find a typed equivalent for %s.%s. "
                    + "You need to either add a constant in %s, or an exclusion in this test.",
                option.getClass().getSimpleName(), option, TypedDriverOption.class.getSimpleName())
            .contains(option);
      }
    }
  }
}
