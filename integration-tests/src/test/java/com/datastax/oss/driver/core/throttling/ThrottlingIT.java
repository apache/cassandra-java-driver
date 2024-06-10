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
package com.datastax.oss.driver.core.throttling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class ThrottlingIT {

  private static final String QUERY = "select * from foo";

  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Test
  public void should_reject_request_when_throttling_by_concurrency() {

    // Add a delay so that requests don't complete during the test
    simulacron
        .cluster()
        .prime(PrimeDsl.when(QUERY).then(PrimeDsl.noRows()).delay(5, TimeUnit.SECONDS));

    int maxConcurrentRequests = 10;
    int maxQueueSize = 10;

    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withClass(
                DefaultDriverOption.REQUEST_THROTTLER_CLASS,
                ConcurrencyLimitingRequestThrottler.class)
            .withInt(
                DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS,
                maxConcurrentRequests)
            .withInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE, maxQueueSize)
            .build();

    try (CqlSession session = SessionUtils.newSession(simulacron, loader)) {

      // Saturate the session and fill the queue
      for (int i = 0; i < maxConcurrentRequests + maxQueueSize; i++) {
        session.executeAsync(QUERY);
      }

      // The next query should be rejected
      Throwable t = catchThrowable(() -> session.execute(QUERY));

      assertThat(t)
          .isInstanceOf(RequestThrottlingException.class)
          .hasMessage(
              "The session has reached its maximum capacity "
                  + "(concurrent requests: 10, queue size: 10)");
    }
  }
}
