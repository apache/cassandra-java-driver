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
package com.datastax.oss.driver.api.core.retry;

import static com.datastax.oss.driver.assertions.Assertions.assertThat;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class PerProfileRetryPolicyIT {

  // Shared across all tests methods.
  public static @ClassRule SimulacronRule simulacron =
      new SimulacronRule(ClusterSpec.builder().withNodes(2));

  public static @ClassRule SessionRule<CqlSession> sessionRule =
      SessionRule.builder(simulacron)
          .withOptions(
              "load-balancing-policy.class = com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy",
              "request.retry-policy.class = DefaultRetryPolicy",
              "profiles.profile1.request.retry-policy.class = \"com.datastax.oss.driver.api.core.retry.PerProfileRetryPolicyIT$NoRetryPolicy\"",
              "profiles.profile2 {}")
          .build();

  private static String QUERY_STRING = "select * from foo";
  private static final SimpleStatement QUERY = SimpleStatement.newInstance(QUERY_STRING);

  @Before
  public void clear() {
    simulacron.cluster().clearLogs();
  }

  @BeforeClass
  public static void setup() {
    // node 0 will return an unavailable to query.
    simulacron
        .cluster()
        .node(0)
        .prime(
            when(QUERY_STRING)
                .then(
                    unavailable(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE, 1, 0)));
    // node 1 will return a valid empty rows response.
    simulacron.cluster().node(1).prime(when(QUERY_STRING).then(noRows()));

    // sanity checks
    DriverContext context = sessionRule.session().getContext();
    DriverConfig config = context.config();
    assertThat(config.getProfiles()).containsKeys("profile1", "profile2");

    assertThat(context.retryPolicies())
        .hasSize(3)
        .containsKeys(DriverConfigProfile.DEFAULT_NAME, "profile1", "profile2");

    RetryPolicy defaultPolicy = context.retryPolicy(DriverConfigProfile.DEFAULT_NAME);
    RetryPolicy policy1 = context.retryPolicy("profile1");
    RetryPolicy policy2 = context.retryPolicy("profile2");
    assertThat(defaultPolicy)
        .isInstanceOf(DefaultRetryPolicy.class)
        .isSameAs(policy2)
        .isNotSameAs(policy1);
    assertThat(policy1).isInstanceOf(NoRetryPolicy.class);
  }

  @Test(expected = UnavailableException.class)
  public void should_use_policy_from_request_profile() {
    // since profile1 uses a NoRetryPolicy, UnavailableException should surface to client.
    sessionRule.session().execute(QUERY.setConfigProfileName("profile1"));
  }

  @Test
  public void should_use_policy_from_config_when_not_configured_in_request_profile() {
    // since profile2 has no configured retry policy, it should defer to configuration which uses
    // DefaultRetryPolicy, which should try request on next host (host 1).
    ResultSet result = sessionRule.session().execute(QUERY.setConfigProfileName("profile2"));

    // expect an unavailable exception to be present in errors.
    List<Map.Entry<Node, Throwable>> errors = result.getExecutionInfo().getErrors();
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0).getValue()).isInstanceOf(UnavailableException.class);

    assertQueryCount(0, 1);
    assertQueryCount(1, 1);
  }

  private void assertQueryCount(int node, int expected) {
    assertThat(
            simulacron
                .cluster()
                .node(node)
                .getLogs()
                .getQueryLogs()
                .stream()
                .filter(l -> l.getQuery().equals(QUERY_STRING)))
        .as("Expected query count to be %d for node %d", expected, node)
        .hasSize(expected);
  }

  // A policy that simply rethrows always.
  public static class NoRetryPolicy implements RetryPolicy {

    public NoRetryPolicy(DriverContext context, String profileName) {}

    @Override
    public RetryDecision onReadTimeout(
        Request request,
        ConsistencyLevel cl,
        int blockFor,
        int received,
        boolean dataPresent,
        int retryCount) {
      return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onWriteTimeout(
        Request request,
        ConsistencyLevel cl,
        WriteType writeType,
        int blockFor,
        int received,
        int retryCount) {
      return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onUnavailable(
        Request request, ConsistencyLevel cl, int required, int alive, int retryCount) {
      return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onRequestAborted(Request request, Throwable error, int retryCount) {
      return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onErrorResponse(
        Request request, CoordinatorException error, int retryCount) {
      return RetryDecision.RETHROW;
    }

    @Override
    public void close() {}
  }
}
