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
package com.datastax.oss.driver.api.core.loadbalancing;

import static com.datastax.oss.driver.assertions.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class PerProfileLoadBalancingPolicyIT {

  // 3 2-node DCs
  public static @ClassRule SimulacronRule simulacron =
      new SimulacronRule(ClusterSpec.builder().withNodes(2, 2, 2));

  // default lb policy should consider dc1 local, profile1 dc3, profile2 empty.
  public static @ClassRule SessionRule<CqlSession> sessionRule =
      SessionRule.builder(simulacron)
          .withOptions(
              "load-balancing-policy.local-datacenter = dc1",
              "profiles.profile1.load-balancing-policy.local-datacenter = dc3",
              "profiles.profile2 = {}")
          .build();

  private static String QUERY_STRING = "select * from foo";
  private static final SimpleStatement QUERY = SimpleStatement.newInstance(QUERY_STRING);

  @Before
  public void clear() {
    simulacron.cluster().clearLogs();
  }

  @BeforeClass
  public static void setup() {
    // sanity checks
    DriverContext context = sessionRule.session().getContext();
    DriverConfig config = context.config();
    assertThat(config.getProfiles()).containsKeys("profile1", "profile2");

    assertThat(context.loadBalancingPolicies())
        .hasSize(3)
        .containsKeys(DriverConfigProfile.DEFAULT_NAME, "profile1", "profile2");

    DefaultLoadBalancingPolicy defaultPolicy =
        (DefaultLoadBalancingPolicy) context.loadBalancingPolicy(DriverConfigProfile.DEFAULT_NAME);
    DefaultLoadBalancingPolicy policy1 =
        (DefaultLoadBalancingPolicy) context.loadBalancingPolicy("profile1");
    DefaultLoadBalancingPolicy policy2 =
        (DefaultLoadBalancingPolicy) context.loadBalancingPolicy("profile2");

    assertThat(defaultPolicy).isSameAs(policy2).isNotSameAs(policy1);

    for (Node node : sessionRule.session().getMetadata().getNodes().values()) {
      // if node is in dc2 it should be ignored, otherwise (dc1, dc3) it should be local.
      NodeDistance expectedDistance =
          node.getDatacenter().equals("dc2") ? NodeDistance.IGNORED : NodeDistance.LOCAL;
      assertThat(node.getDistance()).isEqualTo(expectedDistance);
    }
  }

  @Test
  public void should_use_policy_from_request_profile() {
    // Since profile1 uses dc3 as localDC, only those nodes should receive these queries.
    Statement statement = QUERY.setConfigProfileName("profile1");
    for (int i = 0; i < 10; i++) {
      ResultSet result = sessionRule.session().execute(statement);
      assertThat(result.getExecutionInfo().getCoordinator().getDatacenter()).isEqualTo("dc3");
    }

    assertQueryInDc(0, 0);
    assertQueryInDc(1, 0);
    assertQueryInDc(2, 5);
  }

  @Test
  public void should_use_policy_from_config_when_not_configured_in_request_profile() {
    // Since profile2 does not define an lbp config, it should use default which uses dc1.
    Statement statement = QUERY.setConfigProfileName("profile2");
    for (int i = 0; i < 10; i++) {
      ResultSet result = sessionRule.session().execute(statement);
      assertThat(result.getExecutionInfo().getCoordinator().getDatacenter()).isEqualTo("dc1");
    }

    assertQueryInDc(0, 5);
    assertQueryInDc(1, 0);
    assertQueryInDc(2, 0);
  }

  private void assertQueryInDc(int dc, int expectedPerNode) {
    for (int i = 0; i < 2; i++) {
      assertThat(
              simulacron
                  .cluster()
                  .dc(dc)
                  .node(i)
                  .getLogs()
                  .getQueryLogs()
                  .stream()
                  .filter(l -> l.getQuery().equals(QUERY_STRING)))
          .as("Expected query count to be %d for dc %d", 5, i)
          .hasSize(expectedPerNode);
    }
  }
}
