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
package com.datastax.oss.driver.core;

import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.readTimeout;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class AllNodesFailedIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(2));

  @Test
  public void should_report_multiple_errors_per_node() {
    SIMULACRON_RULE.cluster().prime(when("SELECT foo").then(readTimeout(ONE, 0, 0, false)));
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, MultipleRetryPolicy.class)
            .build();

    try (CqlSession session =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
                .withConfigLoader(loader)
                .build()) {
      // when executing a query.
      session.execute("SELECT foo");
      fail("AllNodesFailedException expected");
    } catch (AllNodesFailedException ex) {
      assertThat(ex.getAllErrors()).hasSize(2);
      Iterator<Entry<Node, List<Throwable>>> iterator = ex.getAllErrors().entrySet().iterator();
      // first node should have been tried twice
      Entry<Node, List<Throwable>> node1Errors = iterator.next();
      assertThat(node1Errors.getValue()).hasSize(2);
      // second node should have been tried twice
      Entry<Node, List<Throwable>> node2Errors = iterator.next();
      assertThat(node2Errors.getValue()).hasSize(2);
    }
  }

  public static class MultipleRetryPolicy extends DefaultRetryPolicy {

    public MultipleRetryPolicy(DriverContext context, String profileName) {
      super(context, profileName);
    }

    @Override
    @Deprecated
    public RetryDecision onReadTimeout(
        @NonNull Request request,
        @NonNull ConsistencyLevel cl,
        int blockFor,
        int received,
        boolean dataPresent,
        int retryCount) {
      // retry each node twice
      if (retryCount % 2 == 0) {
        return RetryDecision.RETRY_SAME;
      } else {
        return RetryDecision.RETRY_NEXT;
      }
    }
  }
}
