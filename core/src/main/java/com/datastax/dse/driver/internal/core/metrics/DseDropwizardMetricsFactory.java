/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.metrics;

import com.datastax.dse.driver.DseSessionMetric;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.DropwizardMetricsFactory;
import com.datastax.oss.driver.internal.core.metrics.NoopSessionMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DseDropwizardMetricsFactory extends DropwizardMetricsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DropwizardMetricsFactory.class);

  private final SessionMetricUpdater dseSessionUpdater;
  private final String logPrefix;

  public DseDropwizardMetricsFactory(InternalDriverContext context) {
    super(context);
    logPrefix = context.getSessionName();
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    Set<SessionMetric> enabledSessionMetrics =
        parseSessionMetricPaths(config.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED));
    dseSessionUpdater =
        getMetrics()
            .map(Metrics::getRegistry)
            .map(
                registry ->
                    (SessionMetricUpdater)
                        new DseDropwizardSessionMetricUpdater(
                            enabledSessionMetrics, registry, context))
            .orElse(NoopSessionMetricUpdater.INSTANCE);
  }

  @Override
  public SessionMetricUpdater getSessionUpdater() {
    return dseSessionUpdater;
  }

  @Override
  protected Set<SessionMetric> parseSessionMetricPaths(List<String> paths) {
    Set<SessionMetric> metrics = new HashSet<>();
    for (String path : paths) {
      try {
        metrics.add(DseSessionMetric.fromPath(path));
      } catch (IllegalArgumentException e) {
        try {
          metrics.add(DefaultSessionMetric.fromPath(path));
        } catch (IllegalArgumentException e1) {
          LOG.warn("[{}] Unknown session metric {}, skipping", logPrefix, path);
        }
      }
    }
    return Collections.unmodifiableSet(metrics);
  }
}
