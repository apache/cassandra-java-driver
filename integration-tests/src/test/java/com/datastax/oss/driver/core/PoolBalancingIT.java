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
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class PoolBalancingIT {

  private static final int POOL_SIZE = 2;
  private static final int REQUESTS_PER_CONNECTION = 20;

  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE)
          .withConfigLoader(
              DriverConfigLoader.programmaticBuilder()
                  .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, REQUESTS_PER_CONNECTION)
                  .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, POOL_SIZE)
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  private CountDownLatch done;
  private AtomicReference<Throwable> unexpectedErrorRef;

  @Before
  public void setup() {
    done = new CountDownLatch(1);
    unexpectedErrorRef = new AtomicReference<>();
  }

  @Test
  public void should_balance_requests_across_connections() throws InterruptedException {
    // Generate just the right load to completely fill the pool. All requests should succeed.
    int simultaneousRequests = POOL_SIZE * REQUESTS_PER_CONNECTION;

    for (int i = 0; i < simultaneousRequests; i++) {
      reschedule(null, null);
    }
    SECONDS.sleep(1);
    done.countDown();

    Throwable unexpectedError = unexpectedErrorRef.get();
    if (unexpectedError != null) {
      fail("At least one request failed unexpectedly", unexpectedError);
    }
  }

  private void reschedule(AsyncResultSet asyncResultSet, Throwable throwable) {
    if (done.getCount() == 1) {
      if (throwable != null
          // Actually there is a tiny race condition where pool acquisition may still fail: channel
          // sizes can change as the client is iterating through them, so it can look like they're
          // all full even if there's always a free slot somewhere at every point in time. This will
          // result in NoNodeAvailableException, ignore it.
          && !(throwable instanceof NoNodeAvailableException)) {
        unexpectedErrorRef.compareAndSet(null, throwable);
        // Even a single error is a failure, no need to continue
        done.countDown();
      }
      SESSION_RULE
          .session()
          .executeAsync("SELECT release_version FROM system.local")
          .whenComplete(this::reschedule);
    }
  }
}
