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
package com.datastax.dse.driver.api.core.insights;

import com.datastax.dse.driver.internal.core.insights.InsightsClient;
import com.datastax.dse.driver.internal.core.insights.configuration.InsightsConfiguration;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import io.netty.util.concurrent.DefaultEventExecutor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "6.7.0", description = "DSE 6.7.0 required for Insights support")
public class InsightsClientIT {
  private static final StackTraceElement[] EMPTY_STACK_TRACE = {};

  private static CustomCcmRule ccmRule =
      CustomCcmRule.builder()
          .withNodes(1)
          .withJvmArgs(
              "-Dinsights.service_options_enabled=true",
              "-Dinsights.default_mode=ENABLED_WITH_LOCAL_STORAGE")
          .build();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Test
  public void should_send_insights_startup_event_using_client()
      throws ExecutionException, InterruptedException, TimeoutException {
    // given
    InsightsClient insightsClient =
        InsightsClient.createInsightsClient(
            new InsightsConfiguration(true, 300000L, new DefaultEventExecutor()),
            (InternalDriverContext) sessionRule.session().getContext(),
            EMPTY_STACK_TRACE);

    // when
    insightsClient.sendStartupMessage().toCompletableFuture().get(1000, TimeUnit.SECONDS);

    // then no exception
  }

  @Test
  public void should_send_insights_status_event_using_client()
      throws ExecutionException, InterruptedException, TimeoutException {
    // given
    InsightsClient insightsClient =
        InsightsClient.createInsightsClient(
            new InsightsConfiguration(true, 300000L, new DefaultEventExecutor()),
            (InternalDriverContext) sessionRule.session().getContext(),
            EMPTY_STACK_TRACE);

    // when
    insightsClient.sendStatusMessage().toCompletableFuture().get(1000, TimeUnit.SECONDS);

    // then no exception
  }
}
