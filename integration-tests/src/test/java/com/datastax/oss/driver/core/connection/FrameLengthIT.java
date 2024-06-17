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
package com.datastax.oss.driver.core.connection;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.testinfra.loadbalancing.SortingLoadBalancingPolicy;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class FrameLengthIT {
  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static DriverConfigLoader loader =
      SessionUtils.configLoaderBuilder()
          .withClass(
              DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, SortingLoadBalancingPolicy.class)
          .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, AlwaysRetryAbortedPolicy.class)
          .withBytes(DefaultDriverOption.PROTOCOL_MAX_FRAME_LENGTH, 100 * 1024)
          .build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE).withConfigLoader(loader).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  private static final SimpleStatement LARGE_QUERY =
      SimpleStatement.newInstance("select * from foo").setIdempotent(true);
  private static final SimpleStatement SLOW_QUERY =
      SimpleStatement.newInstance("select * from bar");

  private static final Buffer ONE_HUNDRED_KB = ByteBuffer.allocate(100 * 1024).limit(100 * 1024);

  @BeforeClass
  public static void primeQueries() {
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(LARGE_QUERY.getQuery())
                .then(rows().row("result", ONE_HUNDRED_KB).columnTypes("result", "blob").build()));
    SIMULACRON_RULE
        .cluster()
        .prime(when(SLOW_QUERY.getQuery()).then(noRows()).delay(60, TimeUnit.SECONDS));
  }

  @Test(expected = FrameTooLongException.class)
  public void should_fail_if_request_exceeds_max_frame_length() {
    SESSION_RULE
        .session()
        .execute(SimpleStatement.newInstance("insert into foo (k) values (?)", ONE_HUNDRED_KB));
  }

  @Test
  public void should_fail_if_response_exceeds_max_frame_length() {
    CompletionStage<AsyncResultSet> slowResultFuture =
        SESSION_RULE.session().executeAsync(SLOW_QUERY);
    try {
      SESSION_RULE.session().execute(LARGE_QUERY);
      fail("Expected a " + FrameTooLongException.class.getSimpleName());
    } catch (FrameTooLongException e) {
      // expected
    }
    // Check that the error does not abort other requests on the same connection
    assertThat(slowResultFuture.toCompletableFuture()).isNotCompleted();
  }

  /**
   * A retry policy that always retries aborted requests.
   *
   * <p>We use this to validate that {@link FrameTooLongException} is never passed to the policy (if
   * it were, then this policy would retry it, and the exception thrown to the client would be an
   * {@link AllNodesFailedException}).
   */
  public static class AlwaysRetryAbortedPolicy extends DefaultRetryPolicy {
    public AlwaysRetryAbortedPolicy(DriverContext context, String profileName) {
      super(context, profileName);
    }

    @Override
    public RetryVerdict onRequestAbortedVerdict(
        @NonNull Request request, @NonNull Throwable error, int retryCount) {
      return RetryVerdict.RETRY_NEXT;
    }
  }
}
