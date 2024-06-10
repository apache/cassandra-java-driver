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
package com.datastax.oss.driver.core.loadbalancing;

import static com.datastax.oss.driver.assertions.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.Objects;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class PerProfileLoadBalancingPolicyIT {

  // 3 2-node DCs
  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(2, 2, 2));

  // default lb policy should consider dc1 local, profile1 dc3, profile2 empty.
  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1")
                  .startProfile("profile1")
                  .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc3")
                  .startProfile("profile2")
                  .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "ONE")
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  private static String QUERY_STRING = "select * from foo";
  private static final SimpleStatement QUERY = SimpleStatement.newInstance(QUERY_STRING);

  @Before
  public void clear() {
    SIMULACRON_RULE.cluster().clearLogs();
  }

  @BeforeClass
  public static void setup() {
    // sanity checks
    DriverContext context = SESSION_RULE.session().getContext();
    DriverConfig config = context.getConfig();
    assertThat(config.getProfiles()).containsKeys("profile1", "profile2");

    assertThat(context.getLoadBalancingPolicies())
        .hasSize(3)
        .containsKeys(DriverExecutionProfile.DEFAULT_NAME, "profile1", "profile2");

    LoadBalancingPolicy defaultPolicy =
        context.getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME);
    LoadBalancingPolicy policy1 = context.getLoadBalancingPolicy("profile1");
    LoadBalancingPolicy policy2 = context.getLoadBalancingPolicy("profile2");

    assertThat(defaultPolicy).isSameAs(policy2).isNotSameAs(policy1);

    for (Node node : SESSION_RULE.session().getMetadata().getNodes().values()) {
      // if node is in dc2 it should be ignored, otherwise (dc1, dc3) it should be local.
      NodeDistance expectedDistance =
          Objects.equals(node.getDatacenter(), "dc2") ? NodeDistance.IGNORED : NodeDistance.LOCAL;
      assertThat(node.getDistance()).isEqualTo(expectedDistance);
    }
  }

  @Test
  public void should_use_policy_from_request_profile() {
    // Since profile1 uses dc3 as localDC, only those nodes should receive these queries.
    Statement<?> statement = QUERY.setExecutionProfileName("profile1");
    for (int i = 0; i < 10; i++) {
      ResultSet result = SESSION_RULE.session().execute(statement);
      Node coordinator = result.getExecutionInfo().getCoordinator();
      assertThat(coordinator).isNotNull();
      assertThat(coordinator.getDatacenter()).isEqualTo("dc3");
    }

    assertQueryInDc(0, 0);
    assertQueryInDc(1, 0);
    assertQueryInDc(2, 5);
  }

  @Test
  public void should_use_policy_from_config_when_not_configured_in_request_profile() {
    // Since profile2 does not define an lbp config, it should use default which uses dc1.
    Statement<?> statement = QUERY.setExecutionProfileName("profile2");
    for (int i = 0; i < 10; i++) {
      ResultSet result = SESSION_RULE.session().execute(statement);
      Node coordinator = result.getExecutionInfo().getCoordinator();
      assertThat(coordinator).isNotNull();
      assertThat(coordinator.getDatacenter()).isEqualTo("dc1");
    }

    assertQueryInDc(0, 5);
    assertQueryInDc(1, 0);
    assertQueryInDc(2, 0);
  }

  private void assertQueryInDc(int dc, int expectedPerNode) {
    for (int i = 0; i < 2; i++) {
      assertThat(
              SIMULACRON_RULE.cluster().dc(dc).node(i).getLogs().getQueryLogs().stream()
                  .filter(l -> l.getQuery().equals(QUERY_STRING)))
          .as("Expected query count to be %d for dc %d", 5, i)
          .hasSize(expectedPerNode);
    }
  }
}
