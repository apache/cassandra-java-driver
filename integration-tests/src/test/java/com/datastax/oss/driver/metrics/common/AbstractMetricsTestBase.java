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
package com.datastax.oss.driver.metrics.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Test;

public abstract class AbstractMetricsTestBase {

  @ClassRule public static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final List<String> ENABLED_SESSION_METRICS =
      Stream.of(DefaultSessionMetric.values())
          .map(DefaultSessionMetric::getPath)
          .collect(Collectors.toList());
  private static final List<String> ENABLED_NODE_METRICS =
      Stream.of(DefaultNodeMetric.values())
          .map(DefaultNodeMetric::getPath)
          .collect(Collectors.toList());

  protected Object getMetricRegistry() {
    return null;
  }

  protected abstract String getMetricFactoryClass();

  protected abstract void assertMetrics(CqlSession session);

  protected abstract Collection<?> getRegistryMetrics();

  @Test
  public void should_expose_metrics() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, ENABLED_SESSION_METRICS)
            .withStringList(DefaultDriverOption.METRICS_NODE_ENABLED, ENABLED_NODE_METRICS)
            .withString(DefaultDriverOption.METRICS_FACTORY_CLASS, getMetricFactoryClass())
            .build();
    CqlSessionBuilder builder =
        CqlSession.builder().addContactEndPoints(CCM_RULE.getContactPoints());
    try (CqlSession session =
        (CqlSession)
            builder.withConfigLoader(loader).withMetricRegistry(getMetricRegistry()).build()) {
      for (int i = 0; i < 10; i++) {
        session.execute("SELECT release_version FROM system.local");
      }

      // Should have 10 requests. Assert all applicable metrics.
      assertMetricsSize(getRegistryMetrics());
      assertMetrics(session);
    }
  }

  protected String buildSessionMetricPattern(SessionMetric metric, CqlSession s) {
    return s.getContext().getSessionName() + "\\." + metric.getPath();
  }

  protected String buildNodeMetricPattern(NodeMetric metric, CqlSession s) {
    return s.getContext().getSessionName() + "\\.nodes\\.\\S*\\." + metric.getPath();
  }

  private void assertMetricsSize(Collection<?> metrics) {
    assertThat(metrics).hasSize(ENABLED_SESSION_METRICS.size() + ENABLED_NODE_METRICS.size());
  }
}
