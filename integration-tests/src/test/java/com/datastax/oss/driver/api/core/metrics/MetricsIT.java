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
package com.datastax.oss.driver.api.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class MetricsIT {

  @ClassRule public static CcmRule ccmRule = CcmRule.getInstance();

  @Test
  public void should_expose_metrics() {
    try (CqlSession session =
        SessionUtils.newSession(ccmRule, "metrics.session.enabled = [ cql-requests ]")) {
      for (int i = 0; i < 10; i++) {
        session.execute("SELECT release_version FROM system.local");
      }

      assertThat(session.getMetrics())
          .hasValueSatisfying(
              metrics -> {
                Timer requestsTimer = metrics.getSessionMetric(DefaultSessionMetric.CQL_REQUESTS);
                assertThat(requestsTimer).isNotNull();

                // No need to be very sophisticated, metrics are already covered individually in
                // unit tests.
                assertThat(requestsTimer.getCount()).isEqualTo(10);
              });
    }
  }

  @Test
  public void should_expose_bytes_sent_and_received() {
    try (CqlSession session =
        SessionUtils.newSession(
            ccmRule,
            "metrics.session.enabled = [ bytes-sent, bytes-received ]",
            "metrics.node.enabled = [ bytes-sent, bytes-received ]")) {
      for (int i = 0; i < 10; i++) {
        session.execute("SELECT release_version FROM system.local");
      }

      assertThat(session.getMetrics())
          .hasValueSatisfying(
              metrics -> {
                Meter bytesSent = metrics.getSessionMetric(DefaultSessionMetric.BYTES_SENT);
                assertThat(bytesSent).isNotNull();
                // Can't be precise here as payload can be dependent on protocol version.
                assertThat(bytesSent.getCount()).isGreaterThan(0);

                Meter bytesReceived = metrics.getSessionMetric(DefaultSessionMetric.BYTES_RECEIVED);
                assertThat(bytesReceived).isNotNull();
                assertThat(bytesReceived.getCount()).isGreaterThan(0);

                // get only node in cluster and evaluate its metrics.
                Node node = session.getMetadata().getNodes().values().iterator().next();
                bytesSent = metrics.getNodeMetric(node, DefaultNodeMetric.BYTES_SENT);
                assertThat(bytesSent).isNotNull();
                assertThat(bytesSent.getCount()).isGreaterThan(0);

                bytesReceived = metrics.getNodeMetric(node, DefaultNodeMetric.BYTES_RECEIVED);
                assertThat(bytesReceived).isNotNull();
                assertThat(bytesReceived.getCount()).isGreaterThan(0);
              });
    }
  }

  @Test
  public void should_not_expose_metrics_if_disabled() {
    try (CqlSession session =
        SessionUtils.newSession(
            ccmRule, "metrics.session.enabled = []", "metrics.node.enabled = []")) {
      for (int i = 0; i < 10; i++) {
        session.execute("SELECT release_version FROM system.local");
      }

      assertThat(session.getMetrics()).isEmpty();
    }
  }
}
