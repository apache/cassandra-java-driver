/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core;

import static com.datastax.dse.driver.api.core.config.DseDriverOption.MONITOR_REPORTING_ENABLED;

import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.dse.driver.internal.core.insights.InsightsClient;
import com.datastax.dse.driver.internal.core.insights.configuration.InsightsConfiguration;
import com.datastax.oss.driver.internal.core.context.LifecycleListener;

public class InsightsClientLifecycleListener implements LifecycleListener {
  private static final boolean DEFAULT_INSIGHTS_ENABLED = true;
  private static final long STATUS_EVENT_DELAY_MILLIS = 300000L;
  private final DseDriverContext context;
  private final StackTraceElement[] initCallStackTrace;
  private volatile InsightsClient insightsClient;

  public InsightsClientLifecycleListener(
      DseDriverContext context, StackTraceElement[] initCallStackTrace) {
    this.context = context;
    this.initCallStackTrace = initCallStackTrace;
  }

  @Override
  public void onSessionReady() {
    boolean monitorReportingEnabled =
        context
            .getConfig()
            .getDefaultProfile()
            .getBoolean(MONITOR_REPORTING_ENABLED, DEFAULT_INSIGHTS_ENABLED);

    this.insightsClient =
        InsightsClient.createInsightsClient(
            new InsightsConfiguration(
                monitorReportingEnabled,
                STATUS_EVENT_DELAY_MILLIS,
                context.getNettyOptions().adminEventExecutorGroup().next()),
            context,
            initCallStackTrace);
    insightsClient.sendStartupMessage();
    insightsClient.scheduleStatusMessageSend();
  }

  @Override
  public void close() {
    if (insightsClient != null) {
      insightsClient.shutdown();
    }
  }
}
