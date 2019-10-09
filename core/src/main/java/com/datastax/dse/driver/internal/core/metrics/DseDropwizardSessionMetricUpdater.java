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
