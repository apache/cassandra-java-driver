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
package com.datastax.oss.driver.core.cql.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.reactivex.Flowable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
@Category(ParallelizableTests.class)
public class DefaultReactiveResultSetIT {

  private static CcmRule ccmRule = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @BeforeClass
  public static void initialize() {
    CqlSession session = sessionRule.session();
    session.execute("DROP TABLE IF EXISTS test_reactive_read");
    session.execute("DROP TABLE IF EXISTS test_reactive_write");
    session.checkSchemaAgreement();
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE test_reactive_read (pk int, cc int, v int, PRIMARY KEY ((pk), cc))")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE test_reactive_write (pk int, cc int, v int, PRIMARY KEY ((pk), cc))")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    session.checkSchemaAgreement();
    for (int i = 0; i < 1000; i++) {
      session.execute(
          SimpleStatement.builder("INSERT INTO test_reactive_read (pk, cc, v) VALUES (0, ?, ?)")
              .addPositionalValue(i)
              .addPositionalValue(i)
              .setExecutionProfile(sessionRule.slowProfile())
              .build());
    }
  }

  @Before
  public void truncateTables() throws Exception {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder("TRUNCATE test_reactive_write")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
  }

  @Test
  @DataProvider(
      value = {"1", "10", "100", "999", "1000", "1001", "2000"},
      format = "%m [page size %p[0]]")
  public void should_retrieve_all_rows(int pageSize) {
    DriverExecutionProfile profile =
        sessionRule
            .session()
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, pageSize);
    SimpleStatement statement =
        SimpleStatement.builder("SELECT cc, v FROM test_reactive_read WHERE pk = 0")
            .setExecutionProfile(profile)
            .build();
    ReactiveResultSet rs = sessionRule.session().executeReactive(statement);
    List<ReactiveRow> results = Flowable.fromPublisher(rs).toList().blockingGet();
    assertThat(results.size()).isEqualTo(1000);
    Set<ExecutionInfo> expectedExecInfos = new LinkedHashSet<>();
    for (int i = 0; i < results.size(); i++) {
      ReactiveRow row = results.get(i);
      assertThat(row.getColumnDefinitions()).isNotNull();
      assertThat(row.getExecutionInfo()).isNotNull();
      assertThat(row.wasApplied()).isTrue();
      assertThat(row.getInt("cc")).isEqualTo(i);
      assertThat(row.getInt("v")).isEqualTo(i);
      expectedExecInfos.add(row.getExecutionInfo());
    }

    List<ExecutionInfo> execInfos =
        Flowable.<ExecutionInfo>fromPublisher(rs.getExecutionInfos()).toList().blockingGet();
    // DSE may send an empty page as it can't always know if it's done paging or not yet.
    // See: CASSANDRA-8871. In this case, this page's execution info appears in
    // rs.getExecutionInfos(), but is not present in expectedExecInfos since the page did not
    // contain any rows.
    assertThat(execInfos).containsAll(expectedExecInfos);

    List<ColumnDefinitions> colDefs =
        Flowable.<ColumnDefinitions>fromPublisher(rs.getColumnDefinitions()).toList().blockingGet();
    ReactiveRow first = results.get(0);
    assertThat(colDefs).hasSize(1).containsExactly(first.getColumnDefinitions());

    List<Boolean> wasApplied = Flowable.fromPublisher(rs.wasApplied()).toList().blockingGet();
    assertThat(wasApplied).hasSize(1).containsExactly(first.wasApplied());
  }

  @Test
  public void should_write() {
    SimpleStatement statement =
        SimpleStatement.builder("INSERT INTO test_reactive_write (pk, cc, v) VALUES (?, ?, ?)")
            .addPositionalValue(0)
            .addPositionalValue(1)
            .addPositionalValue(2)
            .setExecutionProfile(sessionRule.slowProfile())
            .build();
    ReactiveResultSet rs = sessionRule.session().executeReactive(statement);
    List<ReactiveRow> results = Flowable.fromPublisher(rs).toList().blockingGet();
    assertThat(results).isEmpty();

    List<ExecutionInfo> execInfos =
        Flowable.<ExecutionInfo>fromPublisher(rs.getExecutionInfos()).toList().blockingGet();
    assertThat(execInfos).hasSize(1);

    List<ColumnDefinitions> colDefs =
        Flowable.<ColumnDefinitions>fromPublisher(rs.getColumnDefinitions()).toList().blockingGet();
    assertThat(colDefs).hasSize(1).containsExactly(EmptyColumnDefinitions.INSTANCE);

    List<Boolean> wasApplied = Flowable.fromPublisher(rs.wasApplied()).toList().blockingGet();
    assertThat(wasApplied).hasSize(1).containsExactly(true);
  }

  @Test
  public void should_write_cas() {
    SimpleStatement statement =
        SimpleStatement.builder(
                "INSERT INTO test_reactive_write (pk, cc, v) VALUES (?, ?, ?) IF NOT EXISTS")
            .addPositionalValue(0)
            .addPositionalValue(1)
            .addPositionalValue(2)
            .setExecutionProfile(sessionRule.slowProfile())
            .build();
    // execute statement for the first time: the insert should succeed and the server should return
    // only one acknowledgement row with just the [applied] column = true
    ReactiveResultSet rs = sessionRule.session().executeReactive(statement);
    List<ReactiveRow> results = Flowable.fromPublisher(rs).toList().blockingGet();
    assertThat(results).hasSize(1);
    ReactiveRow row = results.get(0);
    assertThat(row.getExecutionInfo()).isNotNull();
    assertThat(row.getColumnDefinitions()).hasSize(1);
    assertThat(row.wasApplied()).isTrue();
    assertThat(row.getBoolean("[applied]")).isTrue();

    List<ExecutionInfo> execInfos =
        Flowable.<ExecutionInfo>fromPublisher(rs.getExecutionInfos()).toList().blockingGet();
    assertThat(execInfos).hasSize(1).containsExactly(row.getExecutionInfo());

    List<ColumnDefinitions> colDefs =
        Flowable.<ColumnDefinitions>fromPublisher(rs.getColumnDefinitions()).toList().blockingGet();
    assertThat(colDefs).hasSize(1).containsExactly(row.getColumnDefinitions());

    List<Boolean> wasApplied = Flowable.fromPublisher(rs.wasApplied()).toList().blockingGet();
    assertThat(wasApplied).hasSize(1).containsExactly(row.wasApplied());

    // re-execute same statement: server should return one row with data that failed to be inserted,
    // with [applied] = false
    rs = sessionRule.session().executeReactive(statement);
    results = Flowable.fromPublisher(rs).toList().blockingGet();
    assertThat(results).hasSize(1);
    row = results.get(0);
    assertThat(row.getExecutionInfo()).isNotNull();
    assertThat(row.getColumnDefinitions()).hasSize(4);
    assertThat(row.wasApplied()).isFalse();
    assertThat(row.getBoolean("[applied]")).isFalse();
    assertThat(row.getInt("pk")).isEqualTo(0);
    assertThat(row.getInt("cc")).isEqualTo(1);
    assertThat(row.getInt("v")).isEqualTo(2);

    execInfos =
        Flowable.<ExecutionInfo>fromPublisher(rs.getExecutionInfos()).toList().blockingGet();
    assertThat(execInfos).hasSize(1).containsExactly(row.getExecutionInfo());

    colDefs =
        Flowable.<ColumnDefinitions>fromPublisher(rs.getColumnDefinitions()).toList().blockingGet();
    assertThat(colDefs).hasSize(1).containsExactly(row.getColumnDefinitions());

    wasApplied = Flowable.fromPublisher(rs.wasApplied()).toList().blockingGet();
    assertThat(wasApplied).hasSize(1).containsExactly(row.wasApplied());
  }

  @Test
  public void should_write_batch_cas() {
    BatchStatement batch = createCASBatch();
    CqlSession session = sessionRule.session();
    // execute batch for the first time: all inserts should succeed and the server should return
    // only one acknowledgement row with just the [applied] column = true
    ReactiveResultSet rs = session.executeReactive(batch);
    List<ReactiveRow> results = Flowable.fromPublisher(rs).toList().blockingGet();
    assertThat(results).hasSize(1);
    ReactiveRow row = results.get(0);
    assertThat(row.getExecutionInfo()).isNotNull();
    assertThat(row.getColumnDefinitions()).hasSize(1);
    assertThat(row.wasApplied()).isTrue();
    assertThat(row.getBoolean("[applied]")).isTrue();

    List<ExecutionInfo> execInfos =
        Flowable.<ExecutionInfo>fromPublisher(rs.getExecutionInfos()).toList().blockingGet();
    assertThat(execInfos).hasSize(1).containsExactly(row.getExecutionInfo());

    List<ColumnDefinitions> colDefs =
        Flowable.<ColumnDefinitions>fromPublisher(rs.getColumnDefinitions()).toList().blockingGet();
    assertThat(colDefs).hasSize(1).containsExactly(row.getColumnDefinitions());

    List<Boolean> wasApplied = Flowable.fromPublisher(rs.wasApplied()).toList().blockingGet();
    assertThat(wasApplied).hasSize(1).containsExactly(row.wasApplied());

    // delete 5 out of 10 rows
    partiallyDeleteInsertedRows();

    // re-execute same statement: server should return 5 rows for the 5 failed inserts, each one
    // with [applied] = false
    rs = session.executeReactive(batch);
    results = Flowable.fromPublisher(rs).toList().blockingGet();
    assertThat(results).hasSize(5);
    for (int i = 0; i < 5; i++) {
      row = results.get(i);
      assertThat(row.getExecutionInfo()).isNotNull();
      assertThat(row.getColumnDefinitions()).hasSize(4);
      assertThat(row.wasApplied()).isFalse();
      assertThat(row.getBoolean("[applied]")).isFalse();
      assertThat(row.getInt("pk")).isEqualTo(0);
      assertThat(row.getInt("cc")).isEqualTo(i);
      assertThat(row.getInt("v")).isEqualTo(i + 1);
    }

    execInfos =
        Flowable.<ExecutionInfo>fromPublisher(rs.getExecutionInfos()).toList().blockingGet();
    assertThat(execInfos).hasSize(1).containsExactly(row.getExecutionInfo());

    colDefs =
        Flowable.<ColumnDefinitions>fromPublisher(rs.getColumnDefinitions()).toList().blockingGet();
    assertThat(colDefs).hasSize(1).containsExactly(row.getColumnDefinitions());

    wasApplied = Flowable.fromPublisher(rs.wasApplied()).toList().blockingGet();
    assertThat(wasApplied).hasSize(1).containsExactly(row.wasApplied());
  }

  @NonNull
  private static BatchStatement createCASBatch() {
    // Build a batch with CAS operations on the same partition (conditional batch updates cannot
    // span multiple partitions).
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                "INSERT INTO test_reactive_write (pk, cc, v) VALUES (0, ?, ?) IF NOT EXISTS")
            .setExecutionProfile(sessionRule.slowProfile())
            .build();
    PreparedStatement preparedStatement = sessionRule.session().prepare(insert);
    for (int i = 0; i < 10; i++) {
      builder.addStatement(preparedStatement.bind(i, i + 1));
    }
    return builder.build();
  }

  private static void partiallyDeleteInsertedRows() {
    CqlSession session = sessionRule.session();
    session.execute(" DELETE FROM test_reactive_write WHERE pk = 0 and cc = 5");
    session.execute(" DELETE FROM test_reactive_write WHERE pk = 0 and cc = 6");
    session.execute(" DELETE FROM test_reactive_write WHERE pk = 0 and cc = 7");
    session.execute(" DELETE FROM test_reactive_write WHERE pk = 0 and cc = 8");
    session.execute(" DELETE FROM test_reactive_write WHERE pk = 0 and cc = 9");
  }
}
