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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class SimpleStatementCcmIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 20)
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Rule public TestName name = new TestName();

  private static final String KEY = "test";

  @BeforeClass
  public static void setupSchema() {
    // table where every column forms the primary key.
    SESSION_RULE
        .session()
        .execute(
            SimpleStatement.builder(
                    "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))")
                .setExecutionProfile(SESSION_RULE.slowProfile())
                .build());
    for (int i = 0; i < 100; i++) {
      SESSION_RULE
          .session()
          .execute(
              SimpleStatement.builder("INSERT INTO test (k, v) VALUES (?, ?)")
                  .addPositionalValues(KEY, i)
                  .build());
    }

    // table with simple primary key, single cell.
    SESSION_RULE
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE IF NOT EXISTS test2 (k text primary key, v int)")
                .setExecutionProfile(SESSION_RULE.slowProfile())
                .build());
  }

  @Test
  public void should_use_paging_state_when_copied() {
    Statement<?> st =
        SimpleStatement.builder(String.format("SELECT v FROM test WHERE k='%s'", KEY)).build();
    ResultSet result = SESSION_RULE.session().execute(st);

    // given a query created from a copy of a previous query with paging state from previous queries
    // response.
    st = st.copy(result.getExecutionInfo().getPagingState());

    // when executing that query.
    result = SESSION_RULE.session().execute(st);

    // then the response should start on the page boundary.
    assertThat(result.iterator().next().getInt("v")).isEqualTo(20);
  }

  @Test
  public void should_use_paging_state_when_provided_to_new_statement() {
    Statement<?> st =
        SimpleStatement.builder(String.format("SELECT v FROM test WHERE k='%s'", KEY)).build();
    ResultSet result = SESSION_RULE.session().execute(st);

    // given a query created from a copy of a previous query with paging state from previous queries
    // response.
    st =
        SimpleStatement.builder(String.format("SELECT v FROM test where k='%s'", KEY))
            .setPagingState(result.getExecutionInfo().getPagingState())
            .build();

    // when executing that query.
    result = SESSION_RULE.session().execute(st);

    // then the response should start on the page boundary.
    assertThat(result.iterator().next().getInt("v")).isEqualTo(20);
  }

  @Test
  @Ignore
  public void should_fail_if_using_paging_state_from_different_query() {
    Statement<?> st =
        SimpleStatement.builder("SELECT v FROM test WHERE k=:k").addNamedValue("k", KEY).build();
    ResultSet result = SESSION_RULE.session().execute(st);

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

    SESSION_RULE.session().execute(insert);

    // when retrieving writetime of cell from that insert.
    SimpleStatement select =
        SimpleStatement.builder("SELECT writetime(v) as wv from test2 where k = ?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = SESSION_RULE.session().execute(select);
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
    SESSION_RULE
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
    SESSION_RULE.session().execute(insert);

    // then we should be able to retrieve the data as inserted.
    SimpleStatement select =
        SimpleStatement.builder("select k,v from test2 where k=?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = SESSION_RULE.session().execute(select);
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
    SESSION_RULE.session().execute(insert);

    // then we should be able to retrieve the data as inserted.
    SimpleStatement select =
        SimpleStatement.builder("select k,v from test2 where k=?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = SESSION_RULE.session().execute(select);
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
    SESSION_RULE.session().execute(insert);

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
    SESSION_RULE.session().execute(insert);

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
    SESSION_RULE.session().execute(insert);

    // then we should be able to retrieve the data as inserted.
    SimpleStatement select =
        SimpleStatement.builder("select k,v from test2 where k=:k")
            .addNamedValue("k", name.getMethodName())
            .build();

    ResultSet result = SESSION_RULE.session().execute(select);
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
    SESSION_RULE.session().execute(insert);

    // then we should be able to retrieve the data as inserted.
    SimpleStatement select =
        SimpleStatement.builder("select k,v from test2 where k=:k")
            .addNamedValue("k", name.getMethodName())
            .build();

    ResultSet result = SESSION_RULE.session().execute(select);
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
    SESSION_RULE.session().execute(insert);

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
    Row row = SESSION_RULE.session().execute(statement).one();
    assertThat(row.getLong(0)).isEqualTo(0);
  }

  @Test
  public void should_use_page_size() {
    Statement<?> st = SimpleStatement.builder("SELECT v FROM test").setPageSize(10).build();
    CompletionStage<AsyncResultSet> future = SESSION_RULE.session().executeAsync(st);
    AsyncResultSet result = CompletableFutures.getUninterruptibly(future);

    // Should have only fetched 10 (page size) rows.
    assertThat(result.remaining()).isEqualTo(10);
  }

  @Test
  public void should_not_fail_on_empty_pages() {
    SESSION_RULE
        .session()
        .execute(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");

    SESSION_RULE
        .session()
        .execute("CREATE TABLE IF NOT EXISTS ks.t (pk int, ck int, v int, PRIMARY KEY(pk, ck))");

    for (int i = 0; i < 50; i++) {
      SESSION_RULE.session().execute("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)", i, i, 15);
      SESSION_RULE.session().execute("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)", i, i, 32);
    }

    SESSION_RULE.session().execute("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)", 8, 8, 14);
    SESSION_RULE.session().execute("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)", 11, 11, 14);
    SESSION_RULE.session().execute("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)", 14, 14, 14);

    SimpleStatement st =
        SimpleStatement.newInstance("SELECT * FROM ks.t WHERE v = 14 ALLOW FILTERING");
    st = st.setPageSize(1);
    List<Row> allRows = SESSION_RULE.session().execute(st).all();
    assertThat(allRows.size()).isEqualTo(3);
  }
}
