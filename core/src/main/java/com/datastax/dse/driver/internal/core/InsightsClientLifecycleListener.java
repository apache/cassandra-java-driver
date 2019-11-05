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
package com.datastax.dse.driver.internal.core;

import static com.datastax.dse.driver.api.core.config.DseDriverOption.MONITOR_REPORTING_ENABLED;

import com.datastax.dse.driver.internal.core.insights.InsightsClient;
import com.datastax.dse.driver.internal.core.insights.configuration.InsightsConfiguration;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.LifecycleListener;

public class InsightsClientLifecycleListener implements LifecycleListener {
  private static final boolean DEFAULT_INSIGHTS_ENABLED = true;
  private static final long STATUS_EVENT_DELAY_MILLIS = 300000L;
  private final InternalDriverContext context;
  private final StackTraceElement[] initCallStackTrace;
  private volatile InsightsClient insightsClient;

  public InsightsClientLifecycleListener(
      InternalDriverContext context, StackTraceElement[] initCallStackTrace) {
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
