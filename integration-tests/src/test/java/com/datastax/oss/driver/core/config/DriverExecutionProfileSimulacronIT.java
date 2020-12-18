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
package com.datastax.oss.driver.core.config;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class DriverExecutionProfileSimulacronIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(3));

  @Before
  public void clearPrimes() {
    SIMULACRON_RULE.cluster().clearLogs();
    SIMULACRON_RULE.cluster().clearPrimes(true);
  }

  @Test
  public void should_fail_if_config_profile_specified_doesnt_exist() {
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE)) {
      SimpleStatement statement =
          SimpleStatement.builder("select * from system.local")
              .setExecutionProfileName("IDONTEXIST")
              .build();

      Throwable t = catchThrowable(() -> session.execute(statement));

      assertThat(t)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Unknown profile 'IDONTEXIST'. Check your configuration.");
    }
  }

  @Test
  public void should_use_profile_request_timeout() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(2))
            .startProfile("olap")
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
            .build();
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE, loader)) {
      String query = "mockquery";
      // configure query with delay of 4 seconds.
      SIMULACRON_RULE.cluster().prime(when(query).then(noRows()).delay(4, TimeUnit.SECONDS));

      // Execute query without profile, should timeout with default session timeout (2s).
      try {
        session.execute(query);
        fail("Should have timed out");
      } catch (DriverTimeoutException e) {
        // expected.
      }

      // Execute query with profile, should not timeout since waits up to 10 seconds.
      session.execute(SimpleStatement.builder(query).setExecutionProfileName("olap").build());
    }
  }

  @Test
  public void should_use_profile_default_idempotence() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .startProfile("idem")
            .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
            .build();
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE, loader)) {
      String query = "mockquery";
      // configure query with server error which should invoke onRequestError in retry policy.
      SIMULACRON_RULE.cluster().prime(when(query).then(serverError("fail")));

      // Execute query without profile, should fail because couldn't be retried.
      try {
        session.execute(query);
        fail("Should have failed with server error");
      } catch (ServerError e) {
        // expected.
      }

      // Execute query with profile, should retry on all hosts since query is idempotent.
      Throwable t =
          catchThrowable(
              () ->
                  session.execute(
                      SimpleStatement.builder(query).setExecutionProfileName("idem").build()));

      assertThat(t).isInstanceOf(AllNodesFailedException.class);
    }
  }

  @Test
  public void should_use_profile_consistency() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .startProfile("cl")
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
            .withString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, "LOCAL_SERIAL")
            .build();
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE, loader)) {
      String query = "mockquery";

      // Execute query without profile, should use default CLs (LOCAL_ONE, SERIAL).
      session.execute(query);

      Optional<QueryLog> log =
          SIMULACRON_RULE.cluster().getLogs().getQueryLogs().stream()
              .filter(q -> q.getQuery().equals(query))
              .findFirst();

      assertThat(log)
          .isPresent()
          .hasValueSatisfying(
              (l) -> {
                assertThat(l.getConsistency().toString()).isEqualTo("LOCAL_ONE");
                assertThat(l.getSerialConsistency().toString()).isEqualTo("SERIAL");
              });

      SIMULACRON_RULE.cluster().clearLogs();

      // Execute query with profile, should use profile CLs
      session.execute(SimpleStatement.builder(query).setExecutionProfileName("cl").build());

      log =
          SIMULACRON_RULE.cluster().getLogs().getQueryLogs().stream()
              .filter(q -> q.getQuery().equals(query))
              .findFirst();

      assertThat(log)
          .isPresent()
          .hasValueSatisfying(
              (l) -> {
                assertThat(l.getConsistency().toString()).isEqualTo("LOCAL_QUORUM");
                assertThat(l.getSerialConsistency().toString()).isEqualTo("LOCAL_SERIAL");
              });
    }
  }
}
