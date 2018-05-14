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
package com.datastax.oss.driver.internal.core.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.config.typesafe.TypesafeDriverConfig;
import com.datastax.oss.driver.internal.core.specex.ConstantSpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.specex.NoSpeculativeExecutionPolicy;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mockito;

public class ReflectionTest {

  @Test
  public void should_build_policies_per_profile() {
    String configSource =
        "request.speculative-execution-policy {\n"
            + "  class = ConstantSpeculativeExecutionPolicy\n"
            + "  max-executions = 3\n"
            + "  delay = 100 milliseconds\n"
            + "}\n"
            + "profiles {\n"
            // Inherits from default profile
            + "  profile1 {}\n"
            // Inherits but changes one option
            + "  profile2 { \n"
            + "    request.speculative-execution-policy.max-executions = 2"
            + "  }\n"
            // Same as previous profile, should share the same policy instance
            + "  profile3 { \n"
            + "    request.speculative-execution-policy.max-executions = 2"
            + "  }\n"
            // Completely overrides default profile
            + "  profile4 { \n"
            + "    request.speculative-execution-policy.class = NoSpeculativeExecutionPolicy\n"
            + "  }\n"
            + "}\n";
    DriverContext context = Mockito.mock(DriverContext.class);
    TypesafeDriverConfig config =
        new TypesafeDriverConfig(ConfigFactory.parseString(configSource), new DriverOption[0]);
    Mockito.when(context.config()).thenReturn(config);

    Map<String, SpeculativeExecutionPolicy> policies =
        Reflection.buildFromConfigProfiles(
            context,
            DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY,
            SpeculativeExecutionPolicy.class,
            "com.datastax.oss.driver.internal.core.specex");

    assertThat(policies).hasSize(5);
    SpeculativeExecutionPolicy defaultPolicy = policies.get(DriverConfigProfile.DEFAULT_NAME);
    SpeculativeExecutionPolicy policy1 = policies.get("profile1");
    SpeculativeExecutionPolicy policy2 = policies.get("profile2");
    SpeculativeExecutionPolicy policy3 = policies.get("profile3");
    SpeculativeExecutionPolicy policy4 = policies.get("profile4");
    assertThat(defaultPolicy)
        .isInstanceOf(ConstantSpeculativeExecutionPolicy.class)
        .isSameAs(policy1);
    assertThat(policy2).isInstanceOf(ConstantSpeculativeExecutionPolicy.class).isSameAs(policy3);
    assertThat(policy4).isInstanceOf(NoSpeculativeExecutionPolicy.class);
  }
}
