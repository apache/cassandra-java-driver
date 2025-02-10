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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.Strings;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class QueryTraceIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  private static final SessionRule<CqlSession> SESSION_RULE_SINGLE_TRACE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withBoolean(DefaultDriverOption.REQUEST_TRACE_REPORT_EVERY_PAGE_FETCH, false)
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN =
      RuleChain.outerRule(CCM_RULE).around(SESSION_RULE).around(SESSION_RULE_SINGLE_TRACE);

  @Test
  public void should_not_have_tracing_id_when_tracing_disabled() {
    ExecutionInfo executionInfo =
        SESSION_RULE
            .session()
            .execute("SELECT release_version FROM system.local")
            .getExecutionInfo();

    assertThat(executionInfo.getTracingId()).isNull();

    Throwable t = catchThrowable(executionInfo::getQueryTrace);

    assertThat(t)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Tracing was disabled for this request");
  }

  @Test
  public void should_fetch_trace_when_tracing_enabled() {
    ExecutionInfo executionInfo =
        SESSION_RULE
            .session()
            .execute(
                SimpleStatement.builder("SELECT release_version FROM system.local")
                    .setTracing()
                    .build())
            .getExecutionInfo();

    assertThat(executionInfo.getTracingId()).isNotNull();

    EndPoint contactPoint = CCM_RULE.getContactPoints().iterator().next();
    InetAddress nodeAddress = ((InetSocketAddress) contactPoint.resolve()).getAddress();
    boolean expectPorts =
        CCM_RULE.getCassandraVersion().nextStable().compareTo(Version.V4_0_0) >= 0
            && !CCM_RULE.isDistributionOf(BackendType.DSE);

    QueryTrace queryTrace = executionInfo.getQueryTrace();
    assertThat(queryTrace.getTracingId()).isEqualTo(executionInfo.getTracingId());
    assertThat(queryTrace.getRequestType()).isEqualTo("Execute CQL3 query");
    assertThat(queryTrace.getDurationMicros()).isPositive();
    assertThat(queryTrace.getCoordinatorAddress().getAddress()).isEqualTo(nodeAddress);
    if (expectPorts) {
      Row row =
          SESSION_RULE
              .session()
              .execute(
                  "SELECT coordinator_port FROM system_traces.sessions WHERE session_id = "
                      + queryTrace.getTracingId())
              .one();
      assertThat(row).isNotNull();
      int expectedPort = row.getInt(0);
      assertThat(queryTrace.getCoordinatorAddress().getPort()).isEqualTo(expectedPort);
    } else {
      assertThat(queryTrace.getCoordinatorAddress().getPort()).isEqualTo(0);
    }
    assertThat(queryTrace.getParameters())
        .containsEntry("consistency_level", "LOCAL_ONE")
        .containsEntry("page_size", "5000")
        .containsEntry("query", "SELECT release_version FROM system.local")
        .containsEntry("serial_consistency_level", "SERIAL");
    assertThat(queryTrace.getStartedAt()).isPositive();
    // Don't want to get too deep into event testing because that could change across versions
    assertThat(queryTrace.getEvents()).isNotEmpty();
    InetSocketAddress sourceAddress0 = queryTrace.getEvents().get(0).getSourceAddress();
    assertThat(sourceAddress0).isNotNull();
    assertThat(sourceAddress0.getAddress()).isEqualTo(nodeAddress);
    if (expectPorts) {
      Row row =
          SESSION_RULE
              .session()
              .execute(
                  "SELECT source_port FROM system_traces.events WHERE session_id = "
                      + queryTrace.getTracingId()
                      + " LIMIT 1")
              .one();
      assertThat(row).isNotNull();
      int expectedPort = row.getInt(0);
      assertThat(sourceAddress0.getPort()).isEqualTo(expectedPort);
    } else {
      assertThat(sourceAddress0.getPort()).isEqualTo(0);
    }
  }

  @Test
  public void should_report_trace_once_during_pagination() {
    testTraceDuringPagination(SESSION_RULE_SINGLE_TRACE, 1);
  }

  @Test
  public void should_report_trace_multiple_during_pagination() {
    testTraceDuringPagination(SESSION_RULE, 6);
  }

  private void testTraceDuringPagination(
      SessionRule<CqlSession> sessionRule, int traceEventsCount) {
    String key = setupPaginationTable(sessionRule);

    String cql = "SELECT v0, v1 FROM trace_pagination WHERE k = ?";
    SimpleStatement query = SimpleStatement.builder(cql).setTracing().setPageSize(2).build();
    PreparedStatement preparedStatement = sessionRule.session().prepare(query);
    ResultSet resultSet = sessionRule.session().execute(preparedStatement.bind(key));

    ExecutionInfo executionInfo = resultSet.getExecutionInfo();
    assertThat(executionInfo.getTracingId()).isNotNull();
    QueryTrace queryTrace = executionInfo.getQueryTrace();
    assertThat(queryTrace.getTracingId()).isEqualTo(executionInfo.getTracingId());
    assertThat(queryTrace.getRequestType()).isEqualTo("Execute CQL3 prepared query");

    Iterator<Row> iterator = resultSet.iterator();
    while (iterator.hasNext()) {
      iterator.next(); // iterate over several pages
    }

    // assert that only one event for tracing has been recorded
    await()
        .untilAsserted(
            () -> {
              List<Row> rows =
                  sessionRule.session().execute("SELECT * FROM system_traces.sessions").all()
                      .stream()
                      .filter(row -> isTraceForQuery(row, cql, key))
                      .collect(Collectors.toList());
              assertThat(rows).hasSize(traceEventsCount);
            });
  }

  private String setupPaginationTable(SessionRule<CqlSession> sessionRule) {
    String key = UUID.randomUUID().toString();
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder(
                    "CREATE TABLE IF NOT EXISTS trace_pagination (k text, v0 int, v1 int, PRIMARY KEY(k, v0))")
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
    for (int i = 0; i < 10; i++) {
      sessionRule
          .session()
          .execute(
              SimpleStatement.builder("INSERT INTO trace_pagination (k, v0, v1) VALUES (?, ?, ?)")
                  .addPositionalValues(key, i, i)
                  .build());
    }
    return key;
  }

  private static boolean isTraceForQuery(Row row, String cql, String key) {
    if (!row.getColumnDefinitions().contains("parameters")) {
      return false;
    }
    Map<String, String> queryParams = row.getMap("parameters", String.class, String.class);
    if (queryParams == null || !queryParams.containsKey("query")) {
      return false;
    }
    return queryParams.get("query").contains(cql) && queryParams.containsValue(Strings.quote(key));
  }
}
