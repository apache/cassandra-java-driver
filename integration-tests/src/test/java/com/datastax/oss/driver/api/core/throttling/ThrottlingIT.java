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
package com.datastax.oss.driver.api.core.throttling;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ThrottlingIT {

  private static final String QUERY = "select * from foo";

  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1));
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void should_reject_request_when_throttling_by_concurrency() {

    // Add a delay so that requests don't complete during the test
    simulacron
        .cluster()
        .prime(PrimeDsl.when(QUERY).then(PrimeDsl.noRows()).delay(5, TimeUnit.SECONDS));

    int maxConcurrentRequests = 10;
    int maxQueueSize = 10;

    try (CqlSession session =
        SessionUtils.newSession(
            simulacron,
            "request.throttler.class = " + ConcurrencyLimitingRequestThrottler.class.getName(),
            "request.throttler.max-concurrent-requests = " + maxConcurrentRequests,
            "request.throttler.max-queue-size = " + maxQueueSize)) {

      // Saturate the session and fill the queue
      for (int i = 0; i < maxConcurrentRequests + maxQueueSize; i++) {
        session.executeAsync(QUERY);
      }

      // The next query should be rejected
      thrown.expect(RequestThrottlingException.class);
      thrown.expectMessage(
          "The session has reached its maximum capacity "
              + "(concurrent requests: 10, queue size: 10)");

      session.execute(QUERY);
    }
  }
}
