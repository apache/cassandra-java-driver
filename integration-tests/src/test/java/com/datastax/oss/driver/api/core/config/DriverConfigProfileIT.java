/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class DriverConfigProfileIT {

  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(3));

  @Rule public CcmRule ccm = CcmRule.getInstance();

  @Rule public ExpectedException thrown = ExpectedException.none();

  // TODO: Test with reprepare on all nodes profile configuration

  @Test
  public void should_fail_if_config_profile_specified_doesnt_exist() {
    try (Cluster profileCluster = ClusterUtils.newCluster(simulacron)) {
      Session session = profileCluster.connect();

      SimpleStatement statement =
          SimpleStatement.builder("select * from system.local")
              .withConfigProfileName("IDONTEXIST")
              .build();

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Unknown profile 'IDONTEXIST'. Check your configuration");
      session.execute(statement);
    }
  }

  @Test
  public void should_use_profile_request_timeout() {
    try (Cluster profileCluster =
        ClusterUtils.newCluster(simulacron, "profiles.olap.request.timeout = 10s")) {
      String query = "mockquery";
      // configure query with delay of 2 seconds.
      simulacron.cluster().prime(when(query).then(noRows()).delay(1, TimeUnit.SECONDS));
      Session session = profileCluster.connect();

      // Execute query without profile, should timeout with default (0.5s).
      try {
        session.execute(query);
        fail("Should have timed out");
      } catch (DriverTimeoutException e) {
        // expected.
      }

      // Execute query with profile, should not timeout since waits up to 10 seconds.
      session.execute(SimpleStatement.builder(query).withConfigProfileName("olap").build());
    }
  }

  @Test
  public void should_use_profile_default_idempotence() {
    try (Cluster profileCluster =
        ClusterUtils.newCluster(simulacron, "profiles.idem.request.default-idempotence = true")) {
      String query = "mockquery";
      // configure query with server error which should invoke onRequestError in retry policy.
      simulacron.cluster().prime(when(query).then(serverError("fail")));

      Session session = profileCluster.connect();

      // Execute query without profile, should fail because couldn't be retried.
      try {
        session.execute(query);
        fail("Should have failed with server error");
      } catch (ServerError e) {
        // expected.
      }

      // Execute query with profile, should retry on all hosts since query is idempotent.
      thrown.expect(AllNodesFailedException.class);
      session.execute(SimpleStatement.builder(query).withConfigProfileName("idem").build());
    }
  }

  @Test
  public void should_use_profile_consistency() {
    try (Cluster profileCluster =
        ClusterUtils.newCluster(
            simulacron,
            "profiles.cl.request.consistency = LOCAL_QUORUM",
            "profiles.cl.request.serial-consistency = LOCAL_SERIAL")) {
      String query = "mockquery";

      Session session = profileCluster.connect();

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
      session.execute(SimpleStatement.builder(query).withConfigProfileName("cl").build());

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
    try (Cluster profileCluster =
        ClusterUtils.newCluster(
            ccm, "request.page-size = 100", "profiles.smallpages.request.page-size = 10")) {

      CqlIdentifier keyspace = ClusterUtils.uniqueKeyspaceId();
      DriverConfigProfile slowProfile = ClusterUtils.slowProfile(profileCluster);
      ClusterUtils.createKeyspace(profileCluster, keyspace, slowProfile);

      Session session = profileCluster.connect(keyspace);

      // load 500 rows (value beyond page size).
      session.execute(
          SimpleStatement.builder(
                  "CREATE TABLE IF NOT EXISTS test (k int, v int, PRIMARY KEY (k,v))")
              .withConfigProfile(slowProfile)
              .build());
      PreparedStatement prepared = session.prepare("INSERT INTO test (k, v) values (0, ?)");
      BatchStatementBuilder bs =
          BatchStatement.builder(BatchType.UNLOGGED).withConfigProfile(slowProfile);
      for (int i = 0; i < 500; i++) {
        bs.addStatement(prepared.bind(i));
      }
      session.execute(bs.build());

      String query = "SELECT * FROM test where k=0";
      // Execute query without profile, should use global page size (100)
      ResultSet result = session.execute(query);
      assertThat(result.getAvailableWithoutFetching()).isEqualTo(100);
      result.fetchNextPage();
      // next fetch should also be 100 pages.
      assertThat(result.getAvailableWithoutFetching()).isEqualTo(200);

      // Execute query with profile, should use profile page size
      result =
          session.execute(
              SimpleStatement.builder(query).withConfigProfileName("smallpages").build());
      assertThat(result.getAvailableWithoutFetching()).isEqualTo(10);
      // next fetch should also be 10 pages.
      result.fetchNextPage();
      assertThat(result.getAvailableWithoutFetching()).isEqualTo(20);

      ClusterUtils.dropKeyspace(profileCluster, keyspace, slowProfile);
    }
  }
}
