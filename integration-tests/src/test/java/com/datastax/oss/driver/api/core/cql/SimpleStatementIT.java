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
package com.datastax.oss.driver.api.core.cql;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class SimpleStatementIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccm)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 20)
                  .build())
          .build();

  private static SessionRule<CqlSession> simulacronSessionRule =
      SessionRule.builder(simulacron).build();

  @ClassRule public static TestRule ccmChain = RuleChain.outerRule(ccm).around(sessionRule);

  @ClassRule
  public static TestRule simulacronChain =
      RuleChain.outerRule(simulacron).around(simulacronSessionRule);

  @Rule public TestName name = new TestName();

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String KEY = "test";

  @BeforeClass
  public static void setupSchema() {
    // table where every column forms the primary key.
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder(
                    "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))")
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
    for (int i = 0; i < 100; i++) {
      sessionRule
          .session()
          .execute(
              SimpleStatement.builder("INSERT INTO test (k, v) VALUES (?, ?)")
                  .addPositionalValues(KEY, i)
                  .build());
    }

    // table with simple primary key, single cell.
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE IF NOT EXISTS test2 (k text primary key, v int)")
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
  }

  @Before
  public void clearPrimes() {
    simulacron.cluster().clearLogs();
    simulacron.cluster().clearPrimes(true);
  }

  @Test
  public void should_use_paging_state_when_copied() {
    Statement<?> st =
        SimpleStatement.builder(String.format("SELECT v FROM test WHERE k='%s'", KEY)).build();
    ResultSet result = sessionRule.session().execute(st);

    // given a query created from a copy of a previous query with paging state from previous queries
    // response.
    st = st.copy(result.getExecutionInfo().getPagingState());

    // when executing that query.
    result = sessionRule.session().execute(st);

    // then the response should start on the page boundary.
    assertThat(result.iterator().next().getInt("v")).isEqualTo(20);
  }

  @Test
  public void should_use_paging_state_when_provided_to_new_statement() {
    Statement<?> st =
        SimpleStatement.builder(String.format("SELECT v FROM test WHERE k='%s'", KEY)).build();
    ResultSet result = sessionRule.session().execute(st);

    // given a query created from a copy of a previous query with paging state from previous queries
    // response.
    st =
        SimpleStatement.builder(String.format("SELECT v FROM test where k='%s'", KEY))
            .setPagingState(result.getExecutionInfo().getPagingState())
            .build();

    // when executing that query.
    result = sessionRule.session().execute(st);

    // then the response should start on the page boundary.
    assertThat(result.iterator().next().getInt("v")).isEqualTo(20);
  }

  @Test
  @Ignore
  public void should_fail_if_using_paging_state_from_different_query() {
    Statement<?> st =
        SimpleStatement.builder("SELECT v FROM test WHERE k=:k").addNamedValue("k", KEY).build();
    ResultSet result = sessionRule.session().execute(st);

    // TODO Expect PagingStateException

    // given a new different query and providing the paging state from the previous query
    // then an exception should be thrown indicating incompatible paging state
    SimpleStatement.builder("SELECT v FROM test")
        .setPagingState(result.getExecutionInfo().getPagingState())
        .build();
  }

  @Test
  public void should_use_timestamp_when_set() {
    // given inserting data with a timestamp 40 days in the past.
    long timestamp = System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(40, TimeUnit.DAYS);
    SimpleStatement insert =
        SimpleStatement.builder("INSERT INTO test2 (k, v) values (?, ?)")
            .addPositionalValues(name.getMethodName(), 0)
            .setQueryTimestamp(timestamp)
            .build();

    sessionRule.session().execute(insert);

    // when retrieving writetime of cell from that insert.
    SimpleStatement select =
        SimpleStatement.builder("SELECT writetime(v) as wv from test2 where k = ?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = sessionRule.session().execute(select);
    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    // then the writetime should equal the timestamp provided.
    Row row = rows.iterator().next();
    assertThat(row.getLong("wv")).isEqualTo(timestamp);
  }

  @Test
  @Ignore
  public void should_use_tracing_when_set() {
    // TODO currently there's no way to validate tracing was set since trace id is not set
    // also write test to verify it is not set.
    ResultSet result =
        sessionRule
            .session()
            .execute(SimpleStatement.builder("select * from test").setTracing().build());
  }

  @Test
  public void should_use_positional_values() {
    // given a statement with positional values
    SimpleStatement insert =
        SimpleStatement.builder("INSERT into test2 (k, v) values (?, ?)")
            .addPositionalValue(name.getMethodName())
            .addPositionalValue(4)
            .build();

    // when executing that statement
    sessionRule.session().execute(insert);

    // then we should be able to retrieve the data as inserted.
    SimpleStatement select =
        SimpleStatement.builder("select k,v from test2 where k=?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = sessionRule.session().execute(select);
    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();
    assertThat(row.getString("k")).isEqualTo(name.getMethodName());
    assertThat(row.getInt("v")).isEqualTo(4);
  }

  @Test
  public void should_allow_nulls_in_positional_values() {
    // given a statement with positional values
    SimpleStatement insert =
        SimpleStatement.builder("INSERT into test2 (k, v) values (?, ?)")
            .addPositionalValue(name.getMethodName())
            .addPositionalValue(null)
            .build();

    // when executing that statement
    sessionRule.session().execute(insert);

    // then we should be able to retrieve the data as inserted.
    SimpleStatement select =
        SimpleStatement.builder("select k,v from test2 where k=?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = sessionRule.session().execute(select);
    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();
    assertThat(row.getString("k")).isEqualTo(name.getMethodName());
    assertThat(row.getObject("v")).isNull();
  }

  @Test(expected = InvalidQueryException.class)
  public void should_fail_when_too_many_positional_values_provided() {
    // given a statement with more bound values than anticipated (3 given vs. 2 expected)
    SimpleStatement insert =
        SimpleStatement.builder("INSERT into test (k, v) values (?, ?)")
            .addPositionalValues(KEY, 0, 7)
            .build();

    // when executing that statement
    sessionRule.session().execute(insert);

    // then the server will throw an InvalidQueryException which is thrown up to the client.
  }

  @Test(expected = InvalidQueryException.class)
  public void should_fail_when_not_enough_positional_values_provided() {
    // given a statement with not enough bound values (1 given vs. 2 expected)
    SimpleStatement insert =
        SimpleStatement.builder("SELECT * from test where k = ? and v = ?")
            .addPositionalValue(KEY)
            .build();

    // when executing that statement
    sessionRule.session().execute(insert);

    // then the server will throw an InvalidQueryException which is thrown up to the client.
  }

  @Test
  public void should_use_named_values() {
    // given a statement with named values
    SimpleStatement insert =
        SimpleStatement.builder("INSERT into test2 (k, v) values (:k, :v)")
            .addNamedValue("k", name.getMethodName())
            .addNamedValue("v", 7)
            .build();

    // when executing that statement
    sessionRule.session().execute(insert);

    // then we should be able to retrieve the data as inserted.
    SimpleStatement select =
        SimpleStatement.builder("select k,v from test2 where k=:k")
            .addNamedValue("k", name.getMethodName())
            .build();

    ResultSet result = sessionRule.session().execute(select);
    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();
    assertThat(row.getString("k")).isEqualTo(name.getMethodName());
    assertThat(row.getInt("v")).isEqualTo(7);
  }

  @Test
  public void should_allow_nulls_in_named_values() {
    // given a statement with named values
    SimpleStatement insert =
        SimpleStatement.builder("INSERT into test2 (k, v) values (:k, :v)")
            .addNamedValue("k", name.getMethodName())
            .addNamedValue("v", null)
            .build();

    // when executing that statement
    sessionRule.session().execute(insert);

    // then we should be able to retrieve the data as inserted.
    SimpleStatement select =
        SimpleStatement.builder("select k,v from test2 where k=:k")
            .addNamedValue("k", name.getMethodName())
            .build();

    ResultSet result = sessionRule.session().execute(select);
    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);

    Row row = rows.iterator().next();
    assertThat(row.getString("k")).isEqualTo(name.getMethodName());
    assertThat(row.getObject("v")).isNull();
  }

  @Test(expected = InvalidQueryException.class)
  public void should_fail_when_named_value_missing() {
    // given a statement with a missing named value (:k)
    SimpleStatement insert =
        SimpleStatement.builder("SELECT * from test where k = :k and v = :v")
            .addNamedValue("v", 0)
            .build();

    // when executing that statement
    sessionRule.session().execute(insert);

    // then the server will throw an InvalidQueryException which is thrown up to the client.
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_when_mixing_named_and_positional_values() {
    SimpleStatement.builder("SELECT * from test where k = :k and v = :v")
        .addNamedValue("k", KEY)
        .addPositionalValue(0)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_when_mixing_positional_and_named_values() {
    SimpleStatement.builder("SELECT * from test where k = :k and v = :v")
        .addPositionalValue(0)
        .addNamedValue("k", KEY)
        .build();
  }

  @Test
  public void should_use_positional_value_with_case_sensitive_id() {
    SimpleStatement statement =
        SimpleStatement.builder("SELECT count(*) FROM test2 WHERE k=:\"theKey\"")
            .addNamedValue(CqlIdentifier.fromCql("\"theKey\""), 0)
            .build();
    Row row = sessionRule.session().execute(statement).one();
    assertThat(row.getLong(0)).isEqualTo(0);
  }

  @Test
  public void should_use_page_size() {
    Statement<?> st = SimpleStatement.builder("SELECT v FROM test").setPageSize(10).build();
    CompletionStage<? extends AsyncResultSet> future = sessionRule.session().executeAsync(st);
    AsyncResultSet result = CompletableFutures.getUninterruptibly(future);

    // Should have only fetched 10 (page size) rows.
    assertThat(result.remaining()).isEqualTo(10);
  }

  @Test
  public void should_use_consistencies() {
    SimpleStatement st =
        SimpleStatement.builder("SELECT * FROM test where k = ?")
            .setConsistencyLevel(DefaultConsistencyLevel.TWO)
            .setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
            .build();
    simulacronSessionRule.session().execute(st);

    List<QueryLog> logs = simulacron.cluster().getLogs().getQueryLogs();
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
    simulacron
        .cluster()
        .prime(when("mock query").then(noRows()).delay(1500, TimeUnit.MILLISECONDS));
    SimpleStatement st =
        SimpleStatement.builder("mock query")
            .setTimeout(Duration.ofSeconds(1))
            .setConsistencyLevel(DefaultConsistencyLevel.ONE)
            .build();

    thrown.expect(DriverTimeoutException.class);
    thrown.expectMessage("Query timed out after PT1S");

    simulacronSessionRule.session().execute(st);
  }
}
