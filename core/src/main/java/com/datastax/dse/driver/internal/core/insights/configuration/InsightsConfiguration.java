/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.insights.configuration;

import io.netty.util.concurrent.EventExecutor;

public class InsightsConfiguration {
  private final boolean monitorReportingEnabled;
  private final long statusEventDelayMillis;
  private final EventExecutor executor;

  public InsightsConfiguration(
      boolean monitorReportingEnabled, long statusEventDelayMillis, EventExecutor executor) {
    this.monitorReportingEnabled = monitorReportingEnabled;
    this.statusEventDelayMillis = statusEventDelayMillis;
    this.executor = executor;
  }

  public boolean isMonitorReportingEnabled() {
    return monitorReportingEnabled;
  }

  public long getStatusEventDelayMillis() {
    return statusEventDelayMillis;
  }

  public EventExecutor getExecutor() {
    return executor;
  }
}
