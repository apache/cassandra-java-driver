/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metrics;

import com.codahale.metrics.MetricRegistry;
import com.datastax.dse.driver.DseSessionMetric;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.DropwizardSessionMetricUpdater;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DseDropwizardSessionMetricUpdater extends DropwizardSessionMetricUpdater {

  public DseDropwizardSessionMetricUpdater(
      Set<SessionMetric> enabledMetrics, MetricRegistry registry, InternalDriverContext context) {
    super(enabledMetrics, registry, context);

    initializeHdrTimer(
        DseSessionMetric.CONTINUOUS_CQL_REQUESTS,
        context.getConfig().getDefaultProfile(),
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_HIGHEST,
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_DIGITS,
        DseDriverOption.CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_INTERVAL);
  }
}
