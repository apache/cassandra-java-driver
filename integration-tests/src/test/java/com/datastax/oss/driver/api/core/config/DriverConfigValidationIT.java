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
package com.datastax.oss.driver.api.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.DriverExecutionException;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(ParallelizableTests.class)
public class DriverConfigValidationIT {

  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1));
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void should_fail_to_init_with_invalid_policy() {
    should_fail_to_init_with_invalid_policy("basic.load-balancing-policy.class");
    should_fail_to_init_with_invalid_policy("advanced.reconnection-policy.class");
    should_fail_to_init_with_invalid_policy("advanced.retry-policy.class");
    should_fail_to_init_with_invalid_policy("advanced.speculative-execution-policy.class");
    should_fail_to_init_with_invalid_policy("advanced.auth-provider.class");
    should_fail_to_init_with_invalid_policy("advanced.ssl-engine-factory.class");
    should_fail_to_init_with_invalid_policy("advanced.timestamp-generator.class");
    should_fail_to_init_with_invalid_policy("advanced.request-tracker.class");
    should_fail_to_init_with_invalid_policy("advanced.throttler.class");
    should_fail_to_init_with_invalid_policy("advanced.node-state-listener.class");
    should_fail_to_init_with_invalid_policy("advanced.schema-change-listener.class");
    should_fail_to_init_with_invalid_policy("advanced.address-translator.class");
  }

  private void should_fail_to_init_with_invalid_policy(String classConfigPath) {
    assertThatThrownBy(
            () ->
                SessionUtils.newSession(simulacron, classConfigPath + " = AClassThatDoesNotExist"))
        .satisfies(
            error -> {
              assertThat(error).isInstanceOf(DriverExecutionException.class);
              assertThat(error.getCause())
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessage(
                      "Can't find class AClassThatDoesNotExist "
                          + "(specified by "
                          + classConfigPath
                          + ")");
            });
  }
}
