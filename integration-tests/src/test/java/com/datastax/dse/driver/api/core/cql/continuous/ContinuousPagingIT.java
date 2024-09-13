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
package com.datastax.dse.driver.api.core.cql.continuous;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "5.1.0",
    description = "Continuous paging is only available from 5.1.0 onwards")
@Category(ParallelizableTests.class)
@RunWith(DataProviderRunner.class)
public class ContinuousPagingIT extends ContinuousPagingITBase {

  private static CcmRule ccmRule = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withStringList(
                      DefaultDriverOption.METRICS_SESSION_ENABLED,
                      Collections.singletonList(DseSessionMetric.CONTINUOUS_CQL_REQUESTS.getPath()))
                  .withStringList(
                      DefaultDriverOption.METRICS_NODE_ENABLED,
                      Collections.singletonList(DefaultNodeMetric.CQL_MESSAGES.getPath()))
                  .build())
          .build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @BeforeClass
  public static void setUp() {
    initialize(sessionRule.session(), sessionRule.slowProfile());
  }

  /**
   * Validates {@link ContinuousSession#executeContinuously(Statement)} with a variety of paging
   * options and ensures in all cases the expected number of rows come back.
   *
   * @test_category queries
   * @jira_ticket JAVA-1322
   * @since 1.2.0
   */
  @Test
  @UseDataProvider("pagingOptions")
  public void should_execute_synchronously(Options options) {
    CqlSession session = sessionRule.session();
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v from test where k=?", KEY);
    DriverExecutionProfile profile = options.asProfile(session);
    ContinuousResultSet result =
        session.executeContinuously(statement.setExecutionProfile(profile));
    int i = 0;
    for (Row row : result) {
      assertThat(row.getInt("v")).isEqualTo(i);
      i++;
    }
    assertThat(i).isEqualTo(options.expectedRows);
    validateMetrics(session);
  }

  /**
   * Validates {@link ContinuousSession#executeContinuously(Statement)} with a variety of paging
   * options using a prepared statement and ensures in all cases the expected number of rows come
   * back.
   *
   * @test_category queries
   * @jira_ticket JAVA-1322
   * @since 1.2.0
   */
  @Test
  @UseDataProvider("pagingOptions")
  public void should_execute_prepared_statement_synchronously(Options options) {
    CqlSession session = sessionRule.session();
    DriverExecutionProfile profile = options.asProfile(session);
    ContinuousResultSet result =
        session.executeContinuously(prepared.bind(KEY).setExecutionProfile(profile));
    int i = 0;
    for (Row row : result) {
      assertThat(row.getInt("v")).isEqualTo(i);
      i++;
    }
    assertThat(i).isEqualTo(options.expectedRows);
    validateMetrics(session);
  }

  /**
   * Validates {@link ContinuousSession#executeContinuouslyAsync(Statement)} with a variety of
   * paging options and ensures in all cases the expected number of rows come back and the expected
   * number of pages are received.
   *
   * @test_category queries
   * @jira_ticket JAVA-1322
   * @since 1.2.0
   */
  @Test
  @UseDataProvider("pagingOptions")
  public void should_execute_asynchronously(Options options) {
    CqlSession session = sessionRule.session();
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v from test where k=?", KEY);
    DriverExecutionProfile profile = options.asProfile(session);
    PageStatistics stats =
        CompletableFutures.getUninterruptibly(
            session
                .executeContinuouslyAsync(statement.setExecutionProfile(profile))
                .thenCompose(new AsyncContinuousPagingFunction()));
    assertThat(stats.rows).isEqualTo(options.expectedRows);
    assertThat(stats.pages).isEqualTo(options.expectedPages);
    validateMetrics(session);
  }

  /**
   * Validates that continuous paging is resilient to a schema change being made in the middle of
   * producing pages for the driver if the query was a simple statement.
   *
   * <p>Adds a column 'b' after paging the first row in. This column should not be present in the
   * in-flight queries' rows, but should be present for subsequent queries.
   *
   * @test_category queries
   * @jira_ticket JAVA-1653
   * @since 1.2.0
   */
  @Test
  public void simple_statement_paging_should_be_resilient_to_schema_change() {
    CqlSession session = sessionRule.session();
    SimpleStatement simple = SimpleStatement.newInstance("select * from test_prepare");
    DriverExecutionProfile profile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES, 1)
            .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, 1)
            .withDuration(
                DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE, Duration.ofSeconds(30))
            .withDuration(
                DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, Duration.ofSeconds(30));
    ContinuousResultSet result = session.executeContinuously(simple.setExecutionProfile(profile));
    Iterator<Row> it = result.iterator();
    // First row should have a non-null values.
    Row row0 = it.next();
    assertThat(row0.getString("k")).isNotNull();
    assertThat(row0.isNull("v")).isFalse();
    // Make schema change to add b, its metadata should NOT be present in subsequent rows.
    CqlSession schemaChangeSession =
        SessionUtils.newSession(
            ccmRule, session.getKeyspace().orElseThrow(IllegalStateException::new));
    SimpleStatement statement =
        SimpleStatement.newInstance("ALTER TABLE test_prepare add b int")
            .setExecutionProfile(sessionRule.slowProfile());
    schemaChangeSession.execute(statement);
    schemaChangeSession.checkSchemaAgreement();
    while (it.hasNext()) {
      // Each row should have a value for k and v, but b should not be present as it was not part
      // of the original metadata.
      Row row = it.next();
      assertThat(row.getString("k")).isNotNull();
      assertThat(row.isNull("v")).isFalse();
      assertThat(row.getColumnDefinitions().contains("b")).isFalse();
    }
    // Subsequent queries should contain b in metadata since its a new query.
    result = session.executeContinuously(simple);
    it = result.iterator();
    while (it.hasNext()) {
      Row row = it.next();
      assertThat(row.getString("k")).isNotNull();
      assertThat(row.isNull("v")).isFalse();
      // b should be null, but present in metadata.
      assertThat(row.isNull("b")).isTrue();
      assertThat(row.getColumnDefinitions().contains("b")).isTrue();
    }
  }

  /**
   * Validates that continuous paging is resilient to a schema change being made in the middle of
   * producing pages for the driver if the query was prepared.
   *
   * <p>Drops column 'v' after paging the first row in. This column should still be present in the
   * in-flight queries' rows, but it's value should be null. The column should not be present in
   * subsequent queries.
   *
   * @test_category queries
   * @jira_ticket JAVA-1653
   * @since 1.2.0
   */
  @Test
  public void prepared_statement_paging_should_be_resilient_to_schema_change() {
    CqlSession session = sessionRule.session();
    // Create table and prepare select * query against it.
    session.execute(
        SimpleStatement.newInstance("CREATE TABLE test_prep (k text PRIMARY KEY, v int)")
            .setExecutionProfile(SessionUtils.slowProfile(session)));
    for (int i = 0; i < 100; i++) {
      session.execute(String.format("INSERT INTO test_prep (k, v) VALUES ('foo', %d)", i));
    }
    PreparedStatement prepared = session.prepare("SELECT * FROM test_prep WHERE k = ?");
    DriverExecutionProfile profile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES, 1)
            .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, 1)
            .withDuration(
                DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE, Duration.ofSeconds(30))
            .withDuration(
                DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, Duration.ofSeconds(30));
    ContinuousResultSet result =
        session.executeContinuously(prepared.bind("foo").setExecutionProfile(profile));
    Iterator<Row> it = result.iterator();
    // First row should have a non-null value for v.
    Row row0 = it.next();
    assertThat(row0.getString("k")).isNotNull();
    assertThat(row0.isNull("v")).isFalse();
    // Make schema change to drop v, its metadata should be present, values will be null.
    CqlSession schemaChangeSession =
        SessionUtils.newSession(
            ccmRule, session.getKeyspace().orElseThrow(IllegalStateException::new));
    schemaChangeSession.execute(
        SimpleStatement.newInstance("ALTER TABLE test_prep DROP v;")
            .setExecutionProfile(SessionUtils.slowProfile(schemaChangeSession)));
    while (it.hasNext()) {
      // Each row should have a value for k, v should still be present, but null since column was
      // dropped.
      Row row = it.next();
      assertThat(row.getString("k")).isNotNull();
      if (ccmRule.isDistributionAtMinimalVersion(BackendType.DSE, Version.parse("6.0.0"))) {
        // DSE 6 only, v should be null here since dropped.
        // Not reliable for 5.1 since we may have gotten page queued before schema changed.
        assertThat(row.isNull("v")).isTrue();
      }
      assertThat(row.getColumnDefinitions().contains("v")).isTrue();
    }
    // Subsequent queries should lack v from metadata as it was dropped.
    prepared = session.prepare("SELECT * FROM test_prep WHERE k = ?");
    result = session.executeContinuously(prepared.bind("foo").setExecutionProfile(profile));
    it = result.iterator();
    while (it.hasNext()) {
      Row row = it.next();
      assertThat(row.getString("k")).isNotNull();
      assertThat(row.getColumnDefinitions().contains("v")).isFalse();
    }
  }

  /**
   * Validates that {@link ContinuousResultSet#cancel()} will cancel a continuous paging session by
   * setting maxPagesPerSecond to 1 and sending a cancel immediately and ensuring the total number
   * of rows iterated over is equal to the size of pageSize.
   *
   * <p>Also validates that it is possible to resume the operation using the paging state, as
   * described in the javadocs of {@link ContinuousResultSet#cancel()}.
   *
   * @test_category queries
   * @jira_ticket JAVA-1322
   * @since 1.2.0
   */
  @Test
  public void should_cancel_with_synchronous_paging() {
    CqlSession session = sessionRule.session();
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v from test where k=?", KEY);
    // create options and throttle at a page per second so
    // cancel can go out before the next page is sent.
    // Note that this might not be perfect if there are pauses
    // in the JVM and cancel isn't sent soon enough.
    DriverExecutionProfile profile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES, 1)
            .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, 10)
            .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, 1);
    ContinuousResultSet pagingResult =
        session.executeContinuously(statement.setExecutionProfile(profile));
    pagingResult.cancel();
    int i = 0;
    for (Row row : pagingResult) {
      assertThat(row.getInt("v")).isEqualTo(i);
      i++;
    }
    // Expect only 10 rows as paging was cancelled immediately.
    assertThat(i).isEqualTo(10);
    // attempt to resume the operation from where we left
    ByteBuffer pagingState = pagingResult.getExecutionInfo().getPagingState();
    ContinuousResultSet pagingResultResumed =
        session.executeContinuously(
            statement
                .setExecutionProfile(
                    profile.withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, 0))
                .setPagingState(pagingState));
    for (Row row : pagingResultResumed) {
      assertThat(row.getInt("v")).isEqualTo(i);
      i++;
    }
    assertThat(i).isEqualTo(100);
  }

  /**
   * Validates that {@link ContinuousAsyncResultSet#cancel()} will cancel a continuous paging
   * session by setting maxPagesPerSecond to 1 and sending a cancel after the first page is received
   * and then ensuring that the future returned from {@link
   * ContinuousAsyncResultSet#fetchNextPage()} fails.
   *
   * <p>Also validates that it is possible to resume the operation using the paging state, as
   * described in the javadocs of {@link ContinuousAsyncResultSet#cancel()}.
   *
   * @test_category queries
   * @jira_ticket JAVA-1322
   * @since 1.2.0
   */
  @Test
  public void should_cancel_with_asynchronous_paging() {
    CqlSession session = sessionRule.session();
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v from test where k=?", KEY);
    // create options and throttle at a page per second so
    // cancel can go out before the next page is sent.
    // Note that this might not be perfect if there are pauses
    // in the JVM and cancel isn't sent soon enough.
    DriverExecutionProfile profile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, 10)
            .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, 1);
    CompletionStage<ContinuousAsyncResultSet> future =
        session.executeContinuouslyAsync(statement.setExecutionProfile(profile));
    ContinuousAsyncResultSet pagingResult = CompletableFutures.getUninterruptibly(future);
    // Calling cancel on the previous result should cause the next future to timeout.
    pagingResult.cancel();
    CompletionStage<ContinuousAsyncResultSet> fetchNextPageFuture = pagingResult.fetchNextPage();
    try {
      // Expect future to fail since it was cancelled.
      CompletableFutures.getUninterruptibly(fetchNextPageFuture);
      fail("Expected an execution exception since paging was cancelled.");
    } catch (CancellationException e) {
      assertThat(e)
          .hasMessageContaining("Can't get more results")
          .hasMessageContaining("query was cancelled");
    }
    int i = 0;
    for (Row row : pagingResult.currentPage()) {
      assertThat(row.getInt("v")).isEqualTo(i);
      i++;
    }
    // Expect only 10 rows as this is the defined page size.
    assertThat(i).isEqualTo(10);
    // attempt to resume the operation from where we left
    ByteBuffer pagingState = pagingResult.getExecutionInfo().getPagingState();
    future =
        session.executeContinuouslyAsync(
            statement
                .setExecutionProfile(
                    profile.withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, 0))
                .setPagingState(pagingState));
    ContinuousAsyncResultSet pagingResultResumed;
    do {
      pagingResultResumed = CompletableFutures.getUninterruptibly(future);
      for (Row row : pagingResultResumed.currentPage()) {
        assertThat(row.getInt("v")).isEqualTo(i);
        i++;
      }
      if (pagingResultResumed.hasMorePages()) {
        future = pagingResultResumed.fetchNextPage();
      }
    } while (pagingResultResumed.hasMorePages());
    // expect 10 more rows
    assertThat(i).isEqualTo(100);
  }

  /**
   * Validates that {@link ContinuousAsyncResultSet#cancel()} will cancel a continuous paging
   * session and current tracked {@link CompletionStage} tied to the paging session.
   *
   * <p>Also validates that it is possible to resume the operation using the paging state, as
   * described in the javadocs of {@link ContinuousAsyncResultSet#cancel()}.
   *
   * @test_category queries
   * @jira_ticket JAVA-1322
   * @since 1.2.0
   */
  @Test
  public void should_cancel_future_when_cancelling_previous_result() {
    CqlSession session = sessionRule.session();
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v from test where k=?", KEY);
    // create options and throttle at a page per second so
    // cancel can go out before the next page is sent.
    // Note that this might not be perfect if there are pauses
    // in the JVM and cancel isn't sent soon enough.
    DriverExecutionProfile profile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, 10)
            .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, 1);
    CompletionStage<ContinuousAsyncResultSet> future =
        session.executeContinuouslyAsync(statement.setExecutionProfile(profile));
    ContinuousAsyncResultSet pagingResult = CompletableFutures.getUninterruptibly(future);
    CompletionStage<?> fetchNextPageFuture = pagingResult.fetchNextPage();
    // Calling cancel on the previous result should cause the current future to be cancelled.
    pagingResult.cancel();
    assertThat(fetchNextPageFuture.toCompletableFuture().isCancelled()).isTrue();
    try {
      // Expect future to be cancelled since the previous result was cancelled.
      CompletableFutures.getUninterruptibly(fetchNextPageFuture);
      fail("Expected a cancellation exception since previous result was cancelled.");
    } catch (CancellationException ce) {
      // expected
    }
    int i = 0;
    for (Row row : pagingResult.currentPage()) {
      assertThat(row.getInt("v")).isEqualTo(i);
      i++;
    }
    // Expect only 10 rows as this is the defined page size.
    assertThat(i).isEqualTo(10);
    // attempt to resume the operation from where we left
    ByteBuffer pagingState = pagingResult.getExecutionInfo().getPagingState();
    future =
        session.executeContinuouslyAsync(
            statement
                .setExecutionProfile(
                    profile.withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, 0))
                .setPagingState(pagingState));
    ContinuousAsyncResultSet pagingResultResumed;
    do {
      pagingResultResumed = CompletableFutures.getUninterruptibly(future);
      for (Row row : pagingResultResumed.currentPage()) {
        assertThat(row.getInt("v")).isEqualTo(i);
        i++;
      }
      if (pagingResultResumed.hasMorePages()) {
        future = pagingResultResumed.fetchNextPage();
      }
    } while (pagingResultResumed.hasMorePages());
    // expect 10 more rows
    assertThat(i).isEqualTo(100);
  }

  /**
   * Validates that {@link CompletableFuture#cancel(boolean)} will cancel a continuous paging
   * session by setting maxPagesPerSecond to 1 and sending a cancel after the first page is received
   * and then ensuring that the future returned from {@link
   * ContinuousAsyncResultSet#fetchNextPage()} is cancelled.
   *
   * <p>Also validates that it is possible to resume the operation using the paging state, as
   * described in the javadocs of {@link ContinuousAsyncResultSet#cancel()}.
   *
   * @test_category queries
   * @jira_ticket JAVA-1322
   * @since 1.2.0
   */
  @Test
  public void should_cancel_when_future_is_cancelled() {
    CqlSession session = sessionRule.session();
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v from test where k=?", KEY);
    // create options and throttle at a page per second so
    // cancel can go out before the next page is sent.
    // Note that this might not be perfect if there are pauses
    // in the JVM and cancel isn't sent soon enough.
    DriverExecutionProfile profile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, 10)
            .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, 1);
    CompletionStage<ContinuousAsyncResultSet> future =
        session.executeContinuouslyAsync(statement.setExecutionProfile(profile));
    ContinuousAsyncResultSet pagingResult = CompletableFutures.getUninterruptibly(future);
    CompletableFuture<?> fetchNextPageFuture = pagingResult.fetchNextPage().toCompletableFuture();
    fetchNextPageFuture.cancel(false);
    assertThat(fetchNextPageFuture.isCancelled()).isTrue();
    try {
      // Expect cancellation.
      CompletableFutures.getUninterruptibly(fetchNextPageFuture);
      fail("Expected a cancellation exception since future was cancelled.");
    } catch (CancellationException ce) {
      // expected
    }
    int i = 0;
    for (Row row : pagingResult.currentPage()) {
      assertThat(row.getInt("v")).isEqualTo(i);
      i++;
    }
    // Expect only 10 rows as this is the defined page size.
    assertThat(i).isEqualTo(10);
    // attempt to resume the operation from where we left
    ByteBuffer pagingState = pagingResult.getExecutionInfo().getPagingState();
    future =
        session.executeContinuouslyAsync(
            statement
                .setExecutionProfile(
                    profile.withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, 0))
                .setPagingState(pagingState));
    ContinuousAsyncResultSet pagingResultResumed;
    do {
      pagingResultResumed = CompletableFutures.getUninterruptibly(future);
      for (Row row : pagingResultResumed.currentPage()) {
        assertThat(row.getInt("v")).isEqualTo(i);
        i++;
      }
      if (pagingResultResumed.hasMorePages()) {
        future = pagingResultResumed.fetchNextPage();
      }
    } while (pagingResultResumed.hasMorePages());
    // expect 10 more rows
    assertThat(i).isEqualTo(100);
  }

  /**
   * Validates that a client-side timeout is correctly reported to the caller.
   *
   * @test_category queries
   * @jira_ticket JAVA-1390
   * @since 1.2.0
   */
  @Test
  public void should_time_out_when_server_does_not_produce_pages_fast_enough() throws Exception {
    CqlSession session = sessionRule.session();
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v from test where k=?", KEY);
    // Throttle server at a page per second and set client timeout much lower so that the client
    // will experience a timeout.
    // Note that this might not be perfect if there are pauses in the JVM and the timeout
    // doesn't fire soon enough.
    DriverExecutionProfile profile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, 10)
            .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND, 1)
            .withDuration(
                DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, Duration.ofMillis(100));
    CompletionStage<ContinuousAsyncResultSet> future =
        session.executeContinuouslyAsync(statement.setExecutionProfile(profile));
    ContinuousAsyncResultSet pagingResult = CompletableFutures.getUninterruptibly(future);
    try {
      pagingResult.fetchNextPage().toCompletableFuture().get();
      fail("Expected a timeout");
    } catch (ExecutionException e) {
      assertThat(e.getCause())
          .isInstanceOf(DriverTimeoutException.class)
          .hasMessageContaining("Timed out waiting for page 2");
    }
  }

  /**
   * Validates that the driver behaves appropriately when the client gets behind while paging rows
   * in a continuous paging session. The driver should set autoread to false on the channel for that
   * connection until the client consumes enough pages, at which point it will reenable autoread and
   * continue reading.
   *
   * <p>There is not really a direct way to verify that autoread is disabled, but delaying
   * immediately after executing a continuous paging query should produce this effect.
   *
   * @test_category queries
   * @jira_ticket JAVA-1375
   * @since 1.2.0
   */
  @Test
  public void should_resume_reading_when_client_catches_up() {
    CqlSession session = sessionRule.session();
    SimpleStatement statement =
        SimpleStatement.newInstance("SELECT * from test_autoread where k=?", KEY);
    DriverExecutionProfile profile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, 100);
    CompletionStage<ContinuousAsyncResultSet> result =
        session.executeContinuouslyAsync(statement.setExecutionProfile(profile));
    // Defer consuming of rows for a second, this should cause autoread to be disabled.
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    // Start consuming rows, this should cause autoread to be reenabled once we consume some pages.
    PageStatistics stats =
        CompletableFutures.getUninterruptibly(
            result.thenCompose(new AsyncContinuousPagingFunction()));
    // 20k rows in this table.
    assertThat(stats.rows).isEqualTo(20000);
    // 200 * 100 = 20k.
    assertThat(stats.pages).isEqualTo(200);
  }

  private static class PageStatistics {
    int rows;
    int pages;

    PageStatistics(int rows, int pages) {
      this.rows = rows;
      this.pages = pages;
    }
  }

  /**
   * A function that when invoked, will return a transformed future with another {@link
   * AsyncContinuousPagingFunction} wrapping {@link ContinuousAsyncResultSet#fetchNextPage()} if
   * there are more pages, otherwise returns an immediate future that shares {@link PageStatistics}
   * about how many rows were returned and how many pages were encountered.
   *
   * <p>Note that if observe that data is not parsed in order this future fails with an Exception.
   */
  private static class AsyncContinuousPagingFunction
      implements Function<ContinuousAsyncResultSet, CompletionStage<PageStatistics>> {

    private final int rowsSoFar;

    AsyncContinuousPagingFunction() {
      this(0);
    }

    AsyncContinuousPagingFunction(int rowsSoFar) {
      this.rowsSoFar = rowsSoFar;
    }

    @Override
    public CompletionStage<PageStatistics> apply(ContinuousAsyncResultSet input) {
      int rows = rowsSoFar;
      // Iterate over page and ensure data is in order.
      for (Row row : input.currentPage()) {
        int v = row.getInt("v");
        if (v != rows) {
          fail(String.format("Expected v == %d, got %d.", rows, v));
        }
        rows++;
      }
      // If on last page, complete future, otherwise keep iterating.
      if (!input.hasMorePages()) {
        // DSE may send an empty page as it can't always know if it's done paging or not yet.
        // See: CASSANDRA-8871.  In this case, don't count this page.
        int pages = rows == rowsSoFar ? input.pageNumber() - 1 : input.pageNumber();
        CompletableFuture<PageStatistics> future = new CompletableFuture<>();
        future.complete(new PageStatistics(rows, pages));
        return future;
      } else {
        return input.fetchNextPage().thenCompose(new AsyncContinuousPagingFunction(rows));
      }
    }
  }
}
