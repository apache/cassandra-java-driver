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
package com.datastax.oss.driver.core.cql;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.query;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class BoundStatementSimulacronIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Before
  public void clearPrimes() {
    SIMULACRON_RULE.cluster().clearLogs();
    SIMULACRON_RULE.cluster().clearPrimes(true);
  }

  @Test
  public void should_use_consistencies_from_simple_statement() {
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE)) {
      SimpleStatement st =
          SimpleStatement.builder("SELECT * FROM test where k = ?")
              .setConsistencyLevel(DefaultConsistencyLevel.TWO)
              .setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
              .build();
      PreparedStatement prepared = session.prepare(st);
      SIMULACRON_RULE.cluster().clearLogs();
      // since query is unprimed, we use a text value for bind parameter as this is
      // what simulacron expects for unprimed statements.
      session.execute(prepared.bind("0"));

      List<QueryLog> logs = SIMULACRON_RULE.cluster().getLogs().getQueryLogs();
      assertThat(logs).hasSize(1);

      QueryLog log = logs.get(0);

      Message message = log.getFrame().message;
      assertThat(message).isInstanceOf(Execute.class);
      Execute execute = (Execute) message;
      assertThat(execute.options.consistency)
          .isEqualTo(DefaultConsistencyLevel.TWO.getProtocolCode());
      assertThat(execute.options.serialConsistency)
          .isEqualTo(DefaultConsistencyLevel.LOCAL_SERIAL.getProtocolCode());
    }
  }

  @Test
  public void should_use_consistencies() {
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE)) {
      // set consistencies on simple statement, but they will be unused since
      // overridden by bound statement.
      SimpleStatement st =
          SimpleStatement.builder("SELECT * FROM test where k = ?")
              .setConsistencyLevel(DefaultConsistencyLevel.TWO)
              .setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
              .build();
      PreparedStatement prepared = session.prepare(st);
      SIMULACRON_RULE.cluster().clearLogs();
      // since query is unprimed, we use a text value for bind parameter as this is
      // what simulacron expects for unprimed statements.
      session.execute(
          prepared
              .boundStatementBuilder("0")
              .setConsistencyLevel(DefaultConsistencyLevel.THREE)
              .setSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
              .build());

      List<QueryLog> logs = SIMULACRON_RULE.cluster().getLogs().getQueryLogs();
      assertThat(logs).hasSize(1);

      QueryLog log = logs.get(0);

      Message message = log.getFrame().message;
      assertThat(message).isInstanceOf(Execute.class);
      Execute execute = (Execute) message;
      assertThat(execute.options.consistency)
          .isEqualTo(DefaultConsistencyLevel.THREE.getProtocolCode());
      assertThat(execute.options.serialConsistency)
          .isEqualTo(DefaultConsistencyLevel.SERIAL.getProtocolCode());
    }
  }

  @Test
  public void should_use_timeout_from_simple_statement() {
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE)) {
      LinkedHashMap<String, Object> params = new LinkedHashMap<>(ImmutableMap.of("k", 0));
      LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>(ImmutableMap.of("k", "int"));
      SIMULACRON_RULE
          .cluster()
          .prime(
              when(query(
                      "mock query",
                      Lists.newArrayList(
                          com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE),
                      params,
                      paramTypes))
                  .then(noRows())
                  .delay(1500, TimeUnit.MILLISECONDS));
      SimpleStatement st =
          SimpleStatement.builder("mock query")
              .setTimeout(Duration.ofSeconds(1))
              .setConsistencyLevel(DefaultConsistencyLevel.ONE)
              .build();
      PreparedStatement prepared = session.prepare(st);

      Throwable t = catchThrowable(() -> session.execute(prepared.bind(0)));

      assertThat(t)
          .isInstanceOf(DriverTimeoutException.class)
          .hasMessage("Query timed out after PT1S");
    }
  }

  @Test
  public void should_use_timeout() {
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE)) {
      LinkedHashMap<String, Object> params = new LinkedHashMap<>(ImmutableMap.of("k", 0));
      LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>(ImmutableMap.of("k", "int"));
      // set timeout on simple statement, but will be unused since overridden by bound statement.
      SIMULACRON_RULE
          .cluster()
          .prime(
              when(query(
                      "mock query",
                      Lists.newArrayList(
                          com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE),
                      params,
                      paramTypes))
                  .then(noRows())
                  .delay(1500, TimeUnit.MILLISECONDS));
      SimpleStatement st =
          SimpleStatement.builder("mock query")
              .setTimeout(Duration.ofSeconds(1))
              .setConsistencyLevel(DefaultConsistencyLevel.ONE)
              .build();
      PreparedStatement prepared = session.prepare(st);

      Throwable t =
          catchThrowable(
              () -> session.execute(prepared.bind(0).setTimeout(Duration.ofMillis(150))));

      assertThat(t)
          .isInstanceOf(DriverTimeoutException.class)
          .hasMessage("Query timed out after PT0.15S");
    }
  }
}
