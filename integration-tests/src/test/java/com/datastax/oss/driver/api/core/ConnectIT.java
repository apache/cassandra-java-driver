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
package com.datastax.oss.driver.api.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.testinfra.session.TestConfigLoader;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.server.RejectScope;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class ConnectIT {

  @ClassRule
  public static SimulacronRule simulacronRule =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Before
  public void setup() {
    simulacronRule.cluster().acceptConnections();
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_fail_fast_if_contact_points_unreachable_and_reconnection_disabled() {
    // Given
    simulacronRule.cluster().rejectConnections(0, RejectScope.STOP);

    // When
    SessionUtils.newSession(simulacronRule);

    // Then the exception is thrown
  }

  @Test
  public void should_wait_for_contact_points_if_reconnection_enabled() throws Exception {
    // Given
    simulacronRule.cluster().rejectConnections(0, RejectScope.STOP);

    // When
    CompletableFuture<? extends Session> sessionFuture =
        newSessionAsync(
                simulacronRule,
                "advanced.reconnect-on-init = true",
                // Use a short delay so we don't have to wait too long:
                "advanced.reconnection-policy.class = ConstantReconnectionPolicy",
                "advanced.reconnection-policy.base-delay = 500 milliseconds")
            .toCompletableFuture();
    // wait a bit to ensure we have a couple of reconnections, otherwise we might race and allow
    // reconnections before the initial attempt
    TimeUnit.SECONDS.sleep(2);

    // Then
    assertThat(sessionFuture).isNotCompleted();

    // When
    simulacronRule.cluster().acceptConnections();

    // Then this doesn't throw
    Session session = sessionFuture.get(2, TimeUnit.SECONDS);

    session.close();
  }

  @SuppressWarnings("unchecked")
  private CompletionStage<? extends Session> newSessionAsync(
      SimulacronRule serverRule, String... options) {
    return SessionUtils.baseBuilder()
        .addContactPoints(serverRule.getContactPoints())
        .withConfigLoader(new TestConfigLoader(options))
        .buildAsync();
  }
}
