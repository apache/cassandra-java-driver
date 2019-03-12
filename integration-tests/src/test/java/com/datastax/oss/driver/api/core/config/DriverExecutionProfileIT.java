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
package com.datastax.oss.driver.api.core.config;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(ParallelizableTests.class)
public class DriverExecutionProfileIT {

  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(3));

  @Rule public CcmRule ccm = CcmRule.getInstance();

  @Rule public ExpectedException thrown = ExpectedException.none();

  // TODO: Test with reprepare on all nodes profile configuration

  @Test
  public void should_fail_if_config_profile_specified_doesnt_exist() {
    try (CqlSession session = SessionUtils.newSession(simulacron)) {
      SimpleStatement statement =
          SimpleStatement.builder("select * from system.local")
              .setExecutionProfileName("IDONTEXIST")
              .build();

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Unknown profile 'IDONTEXIST'. Check your configuration");
      session.execute(statement);
    }
  }

  @Test
  public void should_use_profile_request_timeout() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(2))
            .withProfile(
                "olap",
                DefaultDriverConfigLoaderBuilder.profileBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                    .build())
            .build();
    try (CqlSession session = SessionUtils.newSession(simulacron, loader)) {
      String query = "mockquery";
      // configure query with delay of 4 seconds.
      simulacron.cluster().prime(when(query).then(noRows()).delay(4, TimeUnit.SECONDS));

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
            .withProfile(
                "idem",
                DefaultDriverConfigLoaderBuilder.profileBuilder()
                    .withBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, true)
                    .build())
            .build();
    try (CqlSession session = SessionUtils.newSession(simulacron, loader)) {
      String query = "mockquery";
      // configure query with server error which should invoke onRequestError in retry policy.
      simulacron.cluster().prime(when(query).then(serverError("fail")));

      // Execute query without profile, should fail because couldn't be retried.
      try {
        session.execute(query);
        fail("Should have failed with server error");
      } catch (ServerError e) {
        // expected.
      }

      // Execute query with profile, should retry on all hosts since query is idempotent.
      thrown.expect(AllNodesFailedException.class);
      session.execute(SimpleStatement.builder(query).setExecutionProfileName("idem").build());
    }
  }

  @Test
  public void should_use_profile_consistency() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withProfile(
                "cl",
                DefaultDriverConfigLoaderBuilder.profileBuilder()
                    .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
                    .withString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, "LOCAL_SERIAL")
                    .build())
            .build();
    try (CqlSession session = SessionUtils.newSession(simulacron, loader)) {
      String query = "mockquery";

      // Execute query without profile, should use default CLs (LOCAL_ONE, SERIAL).
      session.execute(query);

      Optional<QueryLog> log =
          simulacron
              .cluster()
              .getLogs()
              .getQueryLogs()
              .stream()
              .filter(q -> q.getQuery().equals(query))
              .findFirst();

      assertThat(log)
          .isPresent()
          .hasValueSatisfying(
              (l) -> {
                assertThat(l.getConsistency().toString()).isEqualTo("LOCAL_ONE");
                assertThat(l.getSerialConsistency().toString()).isEqualTo("SERIAL");
              });

      simulacron.cluster().clearLogs();

      // Execute query with profile, should use profile CLs
      session.execute(SimpleStatement.builder(query).setExecutionProfileName("cl").build());

      log =
          simulacron
              .cluster()
              .getLogs()
              .getQueryLogs()
              .stream()
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

  @Test
  public void should_use_profile_page_size() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 100)
            .withProfile(
                "smallpages",
                DefaultDriverConfigLoaderBuilder.profileBuilder()
                    .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 10)
                    .build())
            .build();
    try (CqlSession session = SessionUtils.newSession(ccm, loader)) {

      CqlIdentifier keyspace = SessionUtils.uniqueKeyspaceId();
      DriverExecutionProfile slowProfile = SessionUtils.slowProfile(session);
      SessionUtils.createKeyspace(session, keyspace, slowProfile);

      session.execute(String.format("USE %s", keyspace.asCql(false)));

      // load 500 rows (value beyond page size).
      session.execute(
          SimpleStatement.builder(
                  "CREATE TABLE IF NOT EXISTS test (k int, v int, PRIMARY KEY (k,v))")
              .setExecutionProfile(slowProfile)
              .build());
      PreparedStatement prepared = session.prepare("INSERT INTO test (k, v) values (0, ?)");
      BatchStatementBuilder bs =
          BatchStatement.builder(DefaultBatchType.UNLOGGED).setExecutionProfile(slowProfile);
      for (int i = 0; i < 500; i++) {
        bs.addStatement(prepared.bind(i));
      }
      session.execute(bs.build());

      String query = "SELECT * FROM test where k=0";
      // Execute query without profile, should use global page size (100)
      CompletionStage<? extends AsyncResultSet> future = session.executeAsync(query);
      AsyncResultSet result = CompletableFutures.getUninterruptibly(future);
      assertThat(result.remaining()).isEqualTo(100);
      result = CompletableFutures.getUninterruptibly(result.fetchNextPage());
      // next fetch should also be 100 pages.
      assertThat(result.remaining()).isEqualTo(100);

      // Execute query with profile, should use profile page size
      future =
          session.executeAsync(
              SimpleStatement.builder(query).setExecutionProfileName("smallpages").build());
      result = CompletableFutures.getUninterruptibly(future);
      assertThat(result.remaining()).isEqualTo(10);
      // next fetch should also be 10 pages.
      result = CompletableFutures.getUninterruptibly(result.fetchNextPage());
      assertThat(result.remaining()).isEqualTo(10);

      SessionUtils.dropKeyspace(session, keyspace, slowProfile);
    }
  }
}
