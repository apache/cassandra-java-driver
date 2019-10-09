/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.insights;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRuleBuilder;
import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.dse.driver.internal.core.insights.InsightsClient;
import com.datastax.dse.driver.internal.core.insights.configuration.InsightsConfiguration;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
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

  private static SessionRule<DseSession> sessionRule = new DseSessionRuleBuilder(ccmRule).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Test
  public void should_send_insights_startup_event_using_client()
      throws ExecutionException, InterruptedException, TimeoutException {
    // given
    InsightsClient insightsClient =
        InsightsClient.createInsightsClient(
            new InsightsConfiguration(true, 300000L, new DefaultEventExecutor()),
            (DseDriverContext) sessionRule.session().getContext(),
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
            (DseDriverContext) sessionRule.session().getContext(),
            EMPTY_STACK_TRACE);

    // when
    insightsClient.sendStatusMessage().toCompletableFuture().get(1000, TimeUnit.SECONDS);

    // then no exception
  }
}
