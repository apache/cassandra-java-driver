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
package com.datastax.oss.driver.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.Collections;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class DriverConfigValidationIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Test
  public void should_fail_to_init_with_invalid_policy() {
    should_fail_to_init_with_invalid_policy(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS);
    should_fail_to_init_with_invalid_policy(DefaultDriverOption.RECONNECTION_POLICY_CLASS);
    should_fail_to_init_with_invalid_policy(DefaultDriverOption.RETRY_POLICY_CLASS);
    should_fail_to_init_with_invalid_policy(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS);
    should_fail_to_init_with_invalid_policy(DefaultDriverOption.AUTH_PROVIDER_CLASS);
    should_fail_to_init_with_invalid_policy(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS);
    should_fail_to_init_with_invalid_policy(DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS);
    should_fail_to_init_with_invalid_policy(DefaultDriverOption.REQUEST_THROTTLER_CLASS);
    should_fail_to_init_with_invalid_policy(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS);
  }

  @Test
  public void should_fail_to_init_with_invalid_components() {
    should_fail_to_init_with_invalid_components(DefaultDriverOption.REQUEST_TRACKER_CLASSES);
    should_fail_to_init_with_invalid_components(
        DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASSES);
    should_fail_to_init_with_invalid_components(
        DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASSES);
  }

  private void should_fail_to_init_with_invalid_policy(DefaultDriverOption option) {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder().withString(option, "AClassThatDoesNotExist").build();
    assertConfigError(option, loader);
  }

  private void should_fail_to_init_with_invalid_components(DefaultDriverOption option) {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(option, Collections.singletonList("AClassThatDoesNotExist"))
            .build();
    assertConfigError(option, loader);
  }

  private void assertConfigError(DefaultDriverOption option, DriverConfigLoader loader) {
    assertThatThrownBy(() -> SessionUtils.newSession(SIMULACRON_RULE, loader))
        .satisfies(
            error ->
                assertThat(error)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                        "Can't find class AClassThatDoesNotExist "
                            + "(specified by "
                            + option.getPath()
                            + ")"));
  }
}
