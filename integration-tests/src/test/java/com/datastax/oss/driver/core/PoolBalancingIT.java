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
package com.datastax.oss.driver.core;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class PoolBalancingIT {

  private static final int CONNECTIONS = 20;
  private static final int REQUESTS_PER_CONNECTION = 2;

  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE)
          .withConfigLoader(
              DriverConfigLoader.programmaticBuilder()
                  .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, CONNECTIONS)
                  .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, REQUESTS_PER_CONNECTION)
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  @Test
  public void should_balance_requests_across_connections() throws InterruptedException {
    CqlSession session = SESSION_RULE.session();

    // Generate just the right load to completely fill the pool. All requests should succeed.
    int simultaneousRequests = CONNECTIONS * REQUESTS_PER_CONNECTION;
    ExecutorService requestExecutor = Executors.newFixedThreadPool(simultaneousRequests);
    AtomicReference<Throwable> unexpectedErrorRef = new AtomicReference<>();

    ScheduledExecutorService timeoutExecutor = Executors.newScheduledThreadPool(1);
    CountDownLatch done = new CountDownLatch(1);

    for (int i = 0; i < simultaneousRequests; i++) {
      requestExecutor.submit(
          () -> {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                session.execute("SELECT release_version FROM system.local");
              } catch (Throwable t) {
                unexpectedErrorRef.compareAndSet(null, t);
                // Even a single error is a failure, no need to continue
                done.countDown();
              }
            }
          });
    }
    timeoutExecutor.schedule(done::countDown, 5, SECONDS);

    done.await();

    Throwable unexpectedError = unexpectedErrorRef.get();
    if (unexpectedError != null) {
      fail("At least one request failed unexpectedly", unexpectedError);
    }
    requestExecutor.shutdownNow();
    timeoutExecutor.shutdownNow();
  }
}
