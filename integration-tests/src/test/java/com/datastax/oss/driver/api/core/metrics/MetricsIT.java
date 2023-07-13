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
package com.datastax.oss.driver.api.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class MetricsIT {

  @ClassRule public static CcmRule ccmRule = CcmRule.getInstance();

  @Test
  public void should_expose_metrics() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(
                DefaultDriverOption.METRICS_SESSION_ENABLED,
                Collections.singletonList("cql-requests"))
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {
      for (int i = 0; i < 10; i++) {
        session.execute("SELECT release_version FROM system.local");
      }

      // Should have 10 requests, check within 5 seconds as metric increments after
      // caller is notified.
      ConditionChecker.checkThat(
              () -> {
                assertThat(session.getMetrics())
                    .hasValueSatisfying(
                        metrics ->
                            assertThat(
                                    metrics.<Timer>getSessionMetric(
                                        DefaultSessionMetric.CQL_REQUESTS))
                                .hasValueSatisfying(
                                    cqlRequests -> {
                                      // No need to be very sophisticated, metrics are already
                                      // covered individually in unit tests.
                                      assertThat(cqlRequests.getCount()).isEqualTo(10);
                                    }));
              })
          .before(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void should_expose_bytes_sent_and_received() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(
                DefaultDriverOption.METRICS_SESSION_ENABLED,
                Lists.newArrayList("bytes-sent", "bytes-received"))
            .withStringList(
                DefaultDriverOption.METRICS_NODE_ENABLED,
                Lists.newArrayList("bytes-sent", "bytes-received"))
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {
      for (int i = 0; i < 10; i++) {
        session.execute("SELECT release_version FROM system.local");
      }

      assertThat(session.getMetrics())
          .hasValueSatisfying(
              metrics -> {
                assertThat(metrics.<Meter>getSessionMetric(DefaultSessionMetric.BYTES_SENT))
                    .hasValueSatisfying(
                        // Can't be precise here as payload can be dependent on protocol version.
                        bytesSent -> assertThat(bytesSent.getCount()).isGreaterThan(0));
                assertThat(metrics.<Meter>getSessionMetric(DefaultSessionMetric.BYTES_RECEIVED))
                    .hasValueSatisfying(
                        bytesReceived -> assertThat(bytesReceived.getCount()).isGreaterThan(0));

                // get only node in cluster and evaluate its metrics.
                Node node = session.getMetadata().getNodes().values().iterator().next();
                assertThat(metrics.<Meter>getNodeMetric(node, DefaultNodeMetric.BYTES_SENT))
                    .hasValueSatisfying(
                        bytesSent -> assertThat(bytesSent.getCount()).isGreaterThan(0));
                assertThat(metrics.<Meter>getNodeMetric(node, DefaultNodeMetric.BYTES_RECEIVED))
                    .hasValueSatisfying(
                        bytesReceived -> assertThat(bytesReceived.getCount()).isGreaterThan(0));
              });
    }
  }

  @Test
  public void should_not_expose_metrics_if_disabled() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, Collections.emptyList())
            .withStringList(DefaultDriverOption.METRICS_NODE_ENABLED, Collections.emptyList())
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {
      for (int i = 0; i < 10; i++) {
        session.execute("SELECT release_version FROM system.local");
      }

      assertThat(session.getMetrics()).isEmpty();
    }
  }
}
