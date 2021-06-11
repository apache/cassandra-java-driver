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
package com.datastax.oss.driver.core.retry;

import static com.datastax.oss.driver.assertions.Assertions.assertThat;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.QueryCounter;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class PerProfileRetryPolicyIT {

  // Shared across all tests methods.
  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(2));

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withClass(
                      DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                      SortingLoadBalancingPolicy.class)
                  .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, DefaultRetryPolicy.class)
                  .startProfile("profile1")
                  .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, NoRetryPolicy.class)
                  .startProfile("profile2")
                  .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 100)
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  private static String QUERY_STRING = "select * from foo";
  private static final SimpleStatement QUERY = SimpleStatement.newInstance(QUERY_STRING);

  private final QueryCounter counter =
      QueryCounter.builder(SIMULACRON_RULE.cluster())
          .withFilter((l) -> l.getQuery().equals(QUERY_STRING))
          .build();

  @Before
  public void clear() {
    SIMULACRON_RULE.cluster().clearLogs();
  }

  @BeforeClass
  public static void setup() {
    // node 0 will return an unavailable to query.
    SIMULACRON_RULE
        .cluster()
        .node(0)
        .prime(
            when(QUERY_STRING)
                .then(
                    unavailable(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE, 1, 0)));
    // node 1 will return a valid empty rows response.
    SIMULACRON_RULE.cluster().node(1).prime(when(QUERY_STRING).then(noRows()));

    // sanity checks
    DriverContext context = SESSION_RULE.session().getContext();
    DriverConfig config = context.getConfig();
    assertThat(config.getProfiles()).containsKeys("profile1", "profile2");

    assertThat(context.getRetryPolicies())
        .hasSize(3)
        .containsKeys(DriverExecutionProfile.DEFAULT_NAME, "profile1", "profile2");

    RetryPolicy defaultPolicy = context.getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME);
    RetryPolicy policy1 = context.getRetryPolicy("profile1");
    RetryPolicy policy2 = context.getRetryPolicy("profile2");
    assertThat(defaultPolicy)
        .isInstanceOf(DefaultRetryPolicy.class)
        .isSameAs(policy2)
        .isNotSameAs(policy1);
    assertThat(policy1).isInstanceOf(NoRetryPolicy.class);
  }

  @Test(expected = UnavailableException.class)
  public void should_use_policy_from_request_profile() {
    // since profile1 uses a NoRetryPolicy, UnavailableException should surface to client.
    SESSION_RULE.session().execute(QUERY.setExecutionProfileName("profile1"));
  }

  @Test
  public void should_use_policy_from_config_when_not_configured_in_request_profile() {
    // since profile2 has no configured retry policy, it should defer to configuration which uses
    // DefaultRetryPolicy, which should try request on next host (host 1).
    ResultSet result = SESSION_RULE.session().execute(QUERY.setExecutionProfileName("profile2"));

    // expect an unavailable exception to be present in errors.
    List<Map.Entry<Node, Throwable>> errors = result.getExecutionInfo().getErrors();
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0).getValue()).isInstanceOf(UnavailableException.class);

    counter.assertNodeCounts(1, 1);
  }

  // A policy that simply rethrows always.
  public static class NoRetryPolicy implements RetryPolicy {

    @SuppressWarnings("unused")
    public NoRetryPolicy(DriverContext context, String profileName) {}

    @Override
    @Deprecated
    public RetryDecision onReadTimeout(
        @NonNull Request request,
        @NonNull ConsistencyLevel cl,
        int blockFor,
        int received,
        boolean dataPresent,
        int retryCount) {
      return RetryDecision.RETHROW;
    }

    @Override
    @Deprecated
    public RetryDecision onWriteTimeout(
        @NonNull Request request,
        @NonNull ConsistencyLevel cl,
        @NonNull WriteType writeType,
        int blockFor,
        int received,
        int retryCount) {
      return RetryDecision.RETHROW;
    }

    @Override
    @Deprecated
    public RetryDecision onUnavailable(
        @NonNull Request request,
        @NonNull ConsistencyLevel cl,
        int required,
        int alive,
        int retryCount) {
      return RetryDecision.RETHROW;
    }

    @Override
    @Deprecated
    public RetryDecision onRequestAborted(
        @NonNull Request request, @NonNull Throwable error, int retryCount) {
      return RetryDecision.RETHROW;
    }

    @Override
    @Deprecated
    public RetryDecision onErrorResponse(
        @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
      return RetryDecision.RETHROW;
    }

    @Override
    public void close() {}
  }
}
