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
package com.datastax.oss.driver.internal.core.failover;

import static org.junit.Assert.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class DefaultFailoverPolicyIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  @BeforeClass
  public static void setupSchema() {}

  @Test(expected = IllegalArgumentException.class)
  public void should_throw_exception_invalid_profile() throws Exception {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc2")
            .withClass(DefaultDriverOption.FAILOVER_POLICY_CLASS, DefaultFailoverPolicy.class)
            .withString(DefaultDriverOption.FAILOVER_POLICY_PROFILE, "failover")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      session.execute("select * from system.local");
    }
  }

  @Test
  public void should_failover_bad_dc() throws Exception {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc2")
            .withClass(DefaultDriverOption.FAILOVER_POLICY_CLASS, DefaultFailoverPolicy.class)
            .withString(DefaultDriverOption.FAILOVER_POLICY_PROFILE, "failover")
            .startProfile("failover")
            .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1")
            .endProfile()
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      ResultSet rs = session.execute("select * from system.local");
      assertEquals("dc1", rs.getExecutionInfo().getCoordinator().getDatacenter());
      assertEquals(
          "failover", rs.getExecutionInfo().getStatement().getExecutionProfile().getName());
    }
  }

  @Test
  public void should_not_failover_good_dc() throws Exception {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1")
            .withClass(DefaultDriverOption.FAILOVER_POLICY_CLASS, DefaultFailoverPolicy.class)
            .withString(DefaultDriverOption.FAILOVER_POLICY_PROFILE, "failover")
            .startProfile("failover")
            .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc2")
            .endProfile()
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {
      ResultSet rs = session.execute("select * from system.local");
      assertEquals("dc1", rs.getExecutionInfo().getCoordinator().getDatacenter());
      assertEquals(null, rs.getExecutionInfo().getStatement().getExecutionProfileName());
    }
  }
}
