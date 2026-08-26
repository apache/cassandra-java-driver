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
package com.datastax.oss.driver.core.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.codahale.metrics.Counter;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Exercises CEP-59 graceful disconnect (CASSANDRA-21191) against a real cluster: when a node is
 * drained, it sends a GRACEFUL_DISCONNECT event on every registered connection before closing the
 * transport, and the driver must drain its pool to that node and fail over without surfacing any
 * exception to the application.
 *
 * <p>Requires a server that implements the GRACEFUL_DISCONNECT event; on older servers the test is
 * skipped by the version requirement below.
 */
public class GracefulDisconnectIT {

  @ClassRule
  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(2).build();

  private static final String QUERY = "SELECT * FROM system.local";

  @BackendRequirement(
      type = BackendType.CASSANDRA,
      minInclusive = "7.0",
      description = "Graceful disconnect (CEP-59 / CASSANDRA-21191) requires server-side support")
  @Test
  public void should_fail_over_without_disruption_when_node_drains() throws Exception {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withStringList(
                DefaultDriverOption.METRICS_SESSION_ENABLED,
                ImmutableList.of(DefaultSessionMetric.GRACEFUL_DISCONNECTS.getPath()))
            .build();

    try (CqlSession session = SessionUtils.newSession(CCM_RULE, loader)) {

      // Sanity check before the drain:
      session.execute(QUERY);

      // Steady query load for the whole duration of the test, collecting any exception that
      // reaches the application:
      AtomicLong successes = new AtomicLong();
      List<Throwable> failures = new CopyOnWriteArrayList<>();
      AtomicBoolean stopped = new AtomicBoolean();
      Thread load =
          new Thread(
              () -> {
                while (!stopped.get()) {
                  try {
                    session.execute(QUERY);
                    successes.incrementAndGet();
                  } catch (RuntimeException e) {
                    failures.add(e);
                  }
                }
              },
              "graceful-disconnect-load");
      load.start();

      try {
        // Drain node 2: the server stops accepting new requests and sends GRACEFUL_DISCONNECT on
        // every connection registered for it, then closes the transport.
        CCM_RULE.getCcmBridge().nodetool(2, "drain");

        // The driver must have observed the event (this is also the end-to-end check for the
        // session-level metric):
        Counter gracefulDisconnects =
            (Counter)
                session
                    .getMetrics()
                    .orElseThrow(() -> new AssertionError("expected metrics to be enabled"))
                    .getSessionMetric(DefaultSessionMetric.GRACEFUL_DISCONNECTS)
                    .orElseThrow(
                        () -> new AssertionError("expected graceful-disconnects metric to exist"));
        await()
            .atMost(30, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(gracefulDisconnects.getCount()).isGreaterThan(0));

        // Queries must keep succeeding after the drain (load fails over to the other node):
        long successesAfterEvent = successes.get();
        await()
            .atMost(30, TimeUnit.SECONDS)
            .until(() -> successes.get() > successesAfterEvent + 100);
      } finally {
        stopped.set(true);
        load.join(TimeUnit.SECONDS.toMillis(10));
      }

      // The whole point of graceful disconnect: the shutdown must be invisible to the
      // application, no request may fail.
      assertThat(failures).isEmpty();
    }
  }
}
