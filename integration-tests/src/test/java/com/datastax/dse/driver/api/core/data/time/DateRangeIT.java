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
package com.datastax.dse.driver.api.core.data.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category({ParallelizableTests.class})
@DseRequirement(min = "5.1")
public class DateRangeIT {

  private static CcmRule ccmRule = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Rule public TestName testName = new TestName();

  /**
   * Validates that data can be retrieved by primary key where its primary key is a 'DateRangeType'
   * column, and that the data returned properly parses into the expected {@link DateRange}.
   */
  @Test
  public void should_use_date_range_as_primary_key() throws Exception {
    CqlSession session = sessionRule.session();
    String tableName = testName.getMethodName();

    session.execute(
        String.format("CREATE TABLE %s (k 'DateRangeType' PRIMARY KEY, v int)", tableName));
    session.execute(
        String.format("INSERT INTO %s (k, v) VALUES ('[2010-12-03 TO 2010-12-04]', 1)", tableName));
    session.execute(
        String.format(
            "INSERT INTO %s (k, v) VALUES ('[2015-12-03T10:15:30.001Z TO 2016-01-01T00:05:11.967Z]', 2)",
            tableName));

    List<Row> rows = session.execute("SELECT * FROM " + tableName).all();

    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("k", DateRange.class))
        .isEqualTo(DateRange.parse("[2010-12-03 TO 2010-12-04]"));
    assertThat(rows.get(1).get("k", DateRange.class))
        .isEqualTo(DateRange.parse("[2015-12-03T10:15:30.001Z TO 2016-01-01T00:05:11.967Z]"));

    rows =
        session
            .execute(
                String.format(
                    "SELECT * FROM %s WHERE k = '[2015-12-03T10:15:30.001Z TO 2016-01-01T00:05:11.967]'",
                    tableName))
            .all();
    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.get(0).getInt("v")).isEqualTo(2);
  }

  /**
   * Validates that a 'DateRangeType' column can take a variety of {@link DateRange} inputs:
   *
   * <ol>
   *   <li>Upper bound unbounded
   *   <li>Lower bound unbounded
   *   <li>Unbounded
   *   <li>Bounded
   *   <li>null
   *   <li>unset
   * </ol>
   */
  @Test
  public void should_store_date_range() throws Exception {
    CqlSession session = sessionRule.session();
    String tableName = testName.getMethodName();

    session.execute(
        String.format("CREATE TABLE %s (k int PRIMARY KEY, v 'DateRangeType')", tableName));
    session.execute(
        String.format(
            "INSERT INTO %s (k, v) VALUES (1, '[2000-01-01T10:15:30.301Z TO *]')", tableName));
    session.execute(
        String.format("INSERT INTO %s (k, v) VALUES (2, '[2000-02 TO 2000-03]')", tableName));
    session.execute(String.format("INSERT INTO %s (k, v) VALUES (3, '[* TO 2020]')", tableName));
    session.execute(String.format("INSERT INTO %s (k, v) VALUES (4, null)", tableName));
    session.execute(String.format("INSERT INTO %s (k) VALUES (5)", tableName));
    session.execute(String.format("INSERT INTO %s (k, v) VALUES (6, '*')", tableName));

    List<Row> rows = session.execute("SELECT * FROM " + tableName).all();

    assertThat(rows)
        .extracting(input -> input.get("v", DateRange.class))
        .containsOnly(
            DateRange.parse("[2000-01-01T10:15:30.301Z TO *]"),
            DateRange.parse("[2000-02 TO 2000-03]"),
            DateRange.parse("[* TO 2020]"),
            null,
            DateRange.parse("*"));
  }

  /**
   * Validates that if a provided {@link DateRange} for a 'DateRangeType' column has the bounds
   * reversed (lower bound is later than upper bound), then an {@link InvalidQueryException} is
   * thrown.
   */
  @Test
  public void should_disallow_invalid_order() throws Exception {
    CqlSession session = sessionRule.session();
    String tableName = testName.getMethodName();

    session.execute(
        String.format("CREATE TABLE %s (k int PRIMARY KEY, v 'DateRangeType')", tableName));

    assertThatThrownBy(
            () ->
                session.execute(
                    String.format(
                        "INSERT INTO %s (k, v) "
                            + "VALUES (1, '[2020-01-01T10:15:30.009Z TO 2010-01-01T00:05:11.031Z]')",
                        tableName)))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining("Wrong order: 2020-01-01T10:15:30.009Z TO 2010-01-01T00:05:11.031Z")
        .hasMessageContaining(
            "Could not parse date range: [2020-01-01T10:15:30.009Z TO 2010-01-01T00:05:11.031Z]");
  }

  /** Validates that {@link DateRange} can be used in UDT and Tuple types. */
  @Test
  public void should_allow_date_range_in_udt_and_tuple() throws Exception {
    CqlSession session = sessionRule.session();
    String tableName = testName.getMethodName();

    session.execute("CREATE TYPE IF NOT EXISTS test_udt (i int, range 'DateRangeType')");
    session.execute(
        String.format(
            "CREATE TABLE %s (k int PRIMARY KEY, u test_udt, uf frozen<test_udt>, "
                + "t tuple<'DateRangeType', int>, tf frozen<tuple<'DateRangeType', int>>)",
            tableName));
    session.execute(
        String.format(
            "INSERT INTO %s (k, u, uf, t, tf) VALUES ("
                + "1, "
                + "{i: 10, range: '[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]'}, "
                + "{i: 20, range: '[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]'}, "
                + "('[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]', 30), "
                + "('[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]', 40))",
            tableName));

    DateRange expected = DateRange.parse("[2000-01-01T10:15:30.003Z TO 2020-01-01T10:15:30.001Z]");

    List<Row> rows = session.execute("SELECT * FROM " + tableName).all();
    assertThat(rows).hasSize(1);

    UdtValue u = rows.get(0).get("u", UdtValue.class);
    DateRange dateRange = u.get("range", DateRange.class);
    assertThat(dateRange).isEqualTo(expected);
    assertThat(u.getInt("i")).isEqualTo(10);

    u = rows.get(0).get("uf", UdtValue.class);
    dateRange = u.get("range", DateRange.class);
    assertThat(dateRange).isEqualTo(expected);
    assertThat(u.getInt("i")).isEqualTo(20);

    TupleValue t = rows.get(0).get("t", TupleValue.class);
    dateRange = t.get(0, DateRange.class);
    assertThat(dateRange).isEqualTo(expected);
    assertThat(t.getInt(1)).isEqualTo(30);

    t = rows.get(0).get("tf", TupleValue.class);
    dateRange = t.get(0, DateRange.class);
    assertThat(dateRange).isEqualTo(expected);
    assertThat(t.getInt(1)).isEqualTo(40);
  }

  /** Validates that {@link DateRange} can be used in Collection types (Map, Set, List). */
  @Test
  public void should_allow_date_range_in_collections() throws Exception {
    CqlSession session = sessionRule.session();
    String tableName = testName.getMethodName();

    session.execute(
        String.format(
            "CREATE TABLE %s (k int PRIMARY KEY, l list<'DateRangeType'>, s set<'DateRangeType'>, "
                + "dr2i map<'DateRangeType', int>, i2dr map<int, 'DateRangeType'>)",
            tableName));
    session.execute(
        String.format(
            "INSERT INTO %s (k, l, s, i2dr, dr2i) VALUES ("
                + "1, "
                // l
                + "['[2000-01-01T10:15:30.001Z TO 2020]', '[2010-01-01T10:15:30.001Z TO 2020]',"
                + " '2001-01-02'], "
                // s
                + "{'[2000-01-01T10:15:30.001Z TO 2020]', '[2000-01-01T10:15:30.001Z TO 2020]', "
                + "'[2010-01-01T10:15:30.001Z TO 2020]'}, "
                // i2dr
                + "{1: '[2000-01-01T10:15:30.001Z TO 2020]', "
                + "2: '[2010-01-01T10:15:30.001Z TO 2020]'}, "
                // dr2i
                + "{'[2000-01-01T10:15:30.001Z TO 2020]': 1, "
                + "'[2010-01-01T10:15:30.001Z TO 2020]': 2})",
            tableName));

    List<Row> rows = session.execute("SELECT * FROM " + tableName).all();
    assertThat(rows.size()).isEqualTo(1);

    List<DateRange> drList = rows.get(0).getList("l", DateRange.class);
    assertThat(drList.size()).isEqualTo(3);
    assertThat(drList.get(0)).isEqualTo(DateRange.parse("[2000-01-01T10:15:30.001Z TO 2020]"));
    assertThat(drList.get(1)).isEqualTo(DateRange.parse("[2010-01-01T10:15:30.001Z TO 2020]"));
    assertThat(drList.get(2)).isEqualTo(DateRange.parse("2001-01-02"));

    Set<DateRange> drSet = rows.get(0).getSet("s", DateRange.class);
    assertThat(drSet.size()).isEqualTo(2);
    assertThat(drSet)
        .isEqualTo(
            Sets.newHashSet(
                DateRange.parse("[2000-01-01T10:15:30.001Z TO 2020]"),
                DateRange.parse("[2010-01-01T10:15:30.001Z TO 2020]")));

    Map<DateRange, Integer> dr2i = rows.get(0).getMap("dr2i", DateRange.class, Integer.class);
    assertThat(dr2i.size()).isEqualTo(2);
    assertThat((int) dr2i.get(DateRange.parse("[2000-01-01T10:15:30.001Z TO 2020]"))).isEqualTo(1);
    assertThat((int) dr2i.get(DateRange.parse("[2010-01-01T10:15:30.001Z TO 2020]"))).isEqualTo(2);

    Map<Integer, DateRange> i2dr = rows.get(0).getMap("i2dr", Integer.class, DateRange.class);
    assertThat(i2dr.size()).isEqualTo(2);
    assertThat(i2dr.get(1)).isEqualTo(DateRange.parse("[2000-01-01T10:15:30.001Z TO 2020]"));
    assertThat(i2dr.get(2)).isEqualTo(DateRange.parse("[2010-01-01T10:15:30.001Z TO 2020]"));
  }

  /**
   * Validates that a 'DateRangeType' column can take a {@link DateRange} inputs as a prepared
   * statement parameter.
   */
  @Test
  public void should_bind_date_range_in_prepared_statement() throws Exception {
    CqlSession session = sessionRule.session();
    String tableName = testName.getMethodName();

    session.execute(
        String.format("CREATE TABLE %s (k int PRIMARY KEY, v 'DateRangeType')", tableName));
    PreparedStatement statement =
        session.prepare(String.format("INSERT INTO %s (k,v) VALUES(?,?)", tableName));

    DateRange expected = DateRange.parse("[2007-12-03 TO 2007-12]");
    session.execute(statement.bind(1, expected));
    List<Row> rows = session.execute("SELECT * FROM " + tableName).all();
    assertThat(rows.size()).isEqualTo(1);
    DateRange actual = rows.get(0).get("v", DateRange.class);
    assertThat(actual).isEqualTo(expected);
    assertThat(actual.getLowerBound().getPrecision()).isEqualTo(DateRangePrecision.DAY);
    assertThat(actual.getUpperBound())
        .hasValueSatisfying(
            upperBound ->
                assertThat(upperBound.getPrecision()).isEqualTo(DateRangePrecision.MONTH));
    assertThat(actual.toString()).isEqualTo("[2007-12-03 TO 2007-12]");

    expected = DateRange.parse("[* TO *]");
    session.execute(statement.bind(1, expected));
    rows = session.execute("SELECT * FROM " + tableName).all();
    assertThat(rows.size()).isEqualTo(1);
    actual = rows.get(0).get("v", DateRange.class);
    assertThat(actual).isEqualTo(expected);
    assertThat(actual.getLowerBound().isUnbounded()).isTrue();
    assertThat(actual.isSingleBounded()).isFalse();
    assertThat(actual.getUpperBound())
        .hasValueSatisfying(upperBound -> assertThat(upperBound.isUnbounded()).isTrue());
    assertThat(actual.toString()).isEqualTo("[* TO *]");

    expected = DateRange.parse("*");
    session.execute(statement.bind(1, expected));
    rows = session.execute("SELECT * FROM " + tableName).all();
    assertThat(rows.size()).isEqualTo(1);
    actual = rows.get(0).get("v", DateRange.class);
    assertThat(actual).isEqualTo(expected);
    assertThat(actual.getLowerBound().isUnbounded()).isTrue();
    assertThat(actual.isSingleBounded()).isTrue();
    assertThat(actual.toString()).isEqualTo("*");
  }

  /**
   * Validates that 'DateRangeType' columns are retrievable using <code>SELECT JSON</code> queries
   * and that their value representations match their input.
   */
  @Test
  public void should_select_date_range_using_json() throws Exception {
    CqlSession session = sessionRule.session();
    String tableName = testName.getMethodName();

    session.execute(
        String.format("CREATE TABLE %s (k int PRIMARY KEY, v 'DateRangeType')", tableName));
    PreparedStatement statement =
        session.prepare(String.format("INSERT INTO %s (k,v) VALUES(?,?)", tableName));

    DateRange expected = DateRange.parse("[2007-12-03 TO 2007-12]");
    session.execute(statement.bind(1, expected));
    List<Row> rows = session.execute("SELECT JSON * FROM " + tableName).all();
    assertThat(rows.get(0).getString(0))
        .isEqualTo("{\"k\": 1, \"v\": \"[2007-12-03 TO 2007-12]\"}");

    expected = DateRange.parse("[* TO *]");
    session.execute(statement.bind(1, expected));
    rows = session.execute("SELECT JSON * FROM " + tableName).all();
    assertThat(rows.get(0).getString(0)).isEqualTo("{\"k\": 1, \"v\": \"[* TO *]\"}");

    expected = DateRange.parse("*");
    session.execute(statement.bind(1, expected));
    rows = session.execute("SELECT JSON * FROM " + tableName).all();
    assertThat(rows.get(0).getString(0)).isEqualTo("{\"k\": 1, \"v\": \"*\"}");
  }
}
