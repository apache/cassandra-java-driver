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
package com.datastax.oss.driver.core.heartbeat;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.ClassRule;
import org.junit.Test;

/** This test is separate from {@link HeartbeatIT} because it can't be parallelized. */
public class HeartbeatDisabledIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(2));

  @Test
  public void should_not_send_heartbeat_when_disabled() throws InterruptedException {
    // Disable heartbeats entirely, wait longer than the default timeout and make sure we didn't
    // receive any
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, Duration.ofSeconds(0))
            .build();
    try (CqlSession ignored = SessionUtils.newSession(SIMULACRON_RULE, loader)) {
      AtomicInteger heartbeats = registerHeartbeatListener();
      SECONDS.sleep(35);

      assertThat(heartbeats.get()).isZero();
    }
  }

  private AtomicInteger registerHeartbeatListener() {
    AtomicInteger nonControlHeartbeats = new AtomicInteger();
    SIMULACRON_RULE
        .cluster()
        .registerQueryListener(
            (n, l) -> nonControlHeartbeats.incrementAndGet(),
            false,
            (l) -> l.getQuery().equals("OPTIONS"));
    return nonControlHeartbeats;
  }
}
