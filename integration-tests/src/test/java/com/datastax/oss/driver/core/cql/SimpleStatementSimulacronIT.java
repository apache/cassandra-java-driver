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
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class SimpleStatementSimulacronIT {

  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  @Before
  public void clearPrimes() {
    SIMULACRON_RULE.cluster().clearLogs();
    SIMULACRON_RULE.cluster().clearPrimes(true);
  }

  @Test
  public void should_use_consistencies() {
    SimpleStatement st =
        SimpleStatement.builder("SELECT * FROM test where k = ?")
            .setConsistencyLevel(DefaultConsistencyLevel.TWO)
            .setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
            .build();
    SESSION_RULE.session().execute(st);

    List<QueryLog> logs = SIMULACRON_RULE.cluster().getLogs().getQueryLogs();
    assertThat(logs).hasSize(1);

    QueryLog log = logs.get(0);

    Message message = log.getFrame().message;
    assertThat(message).isInstanceOf(Query.class);
    Query query = (Query) message;
    assertThat(query.options.consistency).isEqualTo(DefaultConsistencyLevel.TWO.getProtocolCode());
    assertThat(query.options.serialConsistency)
        .isEqualTo(DefaultConsistencyLevel.LOCAL_SERIAL.getProtocolCode());
  }

  @Test
  public void should_use_timeout() {
    SIMULACRON_RULE
        .cluster()
        .prime(when("mock query").then(noRows()).delay(1500, TimeUnit.MILLISECONDS));
    SimpleStatement st =
        SimpleStatement.builder("mock query")
            .setTimeout(Duration.ofSeconds(1))
            .setConsistencyLevel(DefaultConsistencyLevel.ONE)
            .build();

    Throwable t = catchThrowable(() -> SESSION_RULE.session().execute(st));

    assertThat(t)
        .isInstanceOf(DriverTimeoutException.class)
        .hasMessage("Query timed out after PT1S");
  }
}
