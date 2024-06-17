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
package com.datastax.oss.driver.core.session;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.unavailable;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class ExceptionIT {

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
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  private static String QUERY_STRING = "select * from foo";

  @Before
  public void clear() {
    SIMULACRON_RULE.cluster().clearLogs();
  }

  @Test
  public void should_expose_execution_info_on_exceptions() {
    // Given
    SIMULACRON_RULE
        .cluster()
        .node(0)
        .prime(
            when(QUERY_STRING)
                .then(
                    unavailable(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE, 1, 0)));
    SIMULACRON_RULE
        .cluster()
        .node(1)
        .prime(when(QUERY_STRING).then(PrimeDsl.invalid("Mock error message")));

    // Then
    assertThatThrownBy(() -> SESSION_RULE.session().execute(QUERY_STRING))
        .isInstanceOf(InvalidQueryException.class)
        .satisfies(
            exception -> {
              ExecutionInfo info = ((InvalidQueryException) exception).getExecutionInfo();
              assertThat(info).isNotNull();
              assertThat(info.getCoordinator().getEndPoint().resolve())
                  .isEqualTo(SIMULACRON_RULE.cluster().node(1).inetSocketAddress());
              assertThat(((SimpleStatement) info.getRequest()).getQuery()).isEqualTo(QUERY_STRING);

              // specex disabled => the initial execution completed the response
              assertThat(info.getSpeculativeExecutionCount()).isEqualTo(0);
              assertThat(info.getSuccessfulExecutionIndex()).isEqualTo(0);

              assertThat(info.getTracingId()).isNull();
              assertThat(info.getPagingState()).isNull();
              assertThat(info.getIncomingPayload()).isEmpty();
              assertThat(info.getWarnings()).isEmpty();
              assertThat(info.isSchemaInAgreement()).isTrue();
              assertThat(info.getResponseSizeInBytes())
                  .isEqualTo(info.getCompressedResponseSizeInBytes())
                  .isEqualTo(-1);

              List<Map.Entry<Node, Throwable>> errors = info.getErrors();
              assertThat(errors).hasSize(1);
              Map.Entry<Node, Throwable> entry0 = errors.get(0);
              assertThat(entry0.getKey().getEndPoint().resolve())
                  .isEqualTo(SIMULACRON_RULE.cluster().node(0).inetSocketAddress());
              Throwable node0Exception = entry0.getValue();
              assertThat(node0Exception).isInstanceOf(UnavailableException.class);
              // ExecutionInfo is not exposed for retried errors
              assertThat(((UnavailableException) node0Exception).getExecutionInfo()).isNull();
            });
  }
}
