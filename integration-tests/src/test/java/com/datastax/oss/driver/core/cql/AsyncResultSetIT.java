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
package com.datastax.oss.driver.core.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class AsyncResultSetIT {

  private static final int PAGE_SIZE = 100;
  private static final int ROWS_PER_PARTITION = 1000;
  private static final String PARTITION_KEY1 = "part";
  private static final String PARTITION_KEY2 = "part2";

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, PAGE_SIZE)
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @BeforeClass
  public static void setupSchema() {
    // create table and load data across two partitions so we can test paging across tokens.
    SESSION_RULE
        .session()
        .execute(
            SimpleStatement.builder(
                    "CREATE TABLE IF NOT EXISTS test (k0 text, k1 int, v int, PRIMARY KEY(k0, k1))")
                .setExecutionProfile(SESSION_RULE.slowProfile())
                .build());

    PreparedStatement prepared =
        SESSION_RULE.session().prepare("INSERT INTO test (k0, k1, v) VALUES (?, ?, ?)");

    BatchStatementBuilder batchPart1 = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    BatchStatementBuilder batchPart2 = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    for (int i = 0; i < ROWS_PER_PARTITION; i++) {
      batchPart1.addStatement(prepared.bind(PARTITION_KEY1, i, i));
      batchPart2.addStatement(
          prepared.bind(PARTITION_KEY2, i + ROWS_PER_PARTITION, i + ROWS_PER_PARTITION));
    }

    SESSION_RULE
        .session()
        .execute(batchPart1.setExecutionProfile(SESSION_RULE.slowProfile()).build());
    SESSION_RULE
        .session()
        .execute(batchPart2.setExecutionProfile(SESSION_RULE.slowProfile()).build());
  }

  @Test
  public void should_only_iterate_over_rows_in_current_page() throws Exception {
    // very basic test that just ensures that iterating over an AsyncResultSet only visits the first
    // page.
    CompletionStage<AsyncResultSet> result =
        SESSION_RULE
            .session()
            .executeAsync(
                SimpleStatement.builder("SELECT * FROM test where k0 = ?")
                    .addPositionalValue(PARTITION_KEY1)
                    .build());

    AsyncResultSet rs = result.toCompletableFuture().get();

    // Should only receive rows in page.
    assertThat(rs.remaining()).isEqualTo(PAGE_SIZE);
    assertThat(rs.hasMorePages()).isTrue();

    Iterator<Row> rowIt = rs.currentPage().iterator();
    for (int i = 0; i < PAGE_SIZE; i++) {
      Row row = rowIt.next();
      assertThat(row.getString("k0")).isEqualTo(PARTITION_KEY1);
      assertThat(row.getInt("k1")).isEqualTo(i);
      assertThat(row.getInt("v")).isEqualTo(i);
    }
  }

  @Test
  public void should_iterate_over_all_pages_asynchronously_single_partition() throws Exception {
    // Validates async paging behavior over single partition.
    CompletionStage<PageStatistics> result =
        SESSION_RULE
            .session()
            .executeAsync(
                SimpleStatement.builder("SELECT * FROM test where k0 = ?")
                    .addPositionalValue(PARTITION_KEY1)
                    .build())
            .thenCompose(new AsyncResultSetConsumingFunction());

    PageStatistics stats = result.toCompletableFuture().get();

    assertThat(stats.rows).isEqualTo(ROWS_PER_PARTITION);
    assertThat(stats.pages).isEqualTo((int) Math.ceil(ROWS_PER_PARTITION / (double) PAGE_SIZE));
  }

  @Test
  public void should_iterate_over_all_pages_asynchronously_cross_partition() throws Exception {
    // Validates async paging behavior over a range query.
    CompletionStage<PageStatistics> result =
        SESSION_RULE
            .session()
            .executeAsync("SELECT * FROM test")
            .thenCompose(new AsyncResultSetConsumingFunction());

    PageStatistics stats = result.toCompletableFuture().get();

    assertThat(stats.rows).isEqualTo(ROWS_PER_PARTITION * 2);
    assertThat(stats.pages).isEqualTo((int) Math.ceil(ROWS_PER_PARTITION * 2 / (double) PAGE_SIZE));
  }

  private static class PageStatistics {
    int rows;
    int pages;

    PageStatistics(int rows, int pages) {
      this.rows = rows;
      this.pages = pages;
    }
  }

  private static class AsyncResultSetConsumingFunction
      implements Function<AsyncResultSet, CompletionStage<PageStatistics>> {

    // number of rows paged before exercising this function.
    private final int rowsSoFar;
    // number of pages encountered before exercising this function.
    private final int pagesSoFar;

    AsyncResultSetConsumingFunction() {
      this(0, 0);
    }

    AsyncResultSetConsumingFunction(int rowsSoFar, int pagesSoFar) {
      this.rowsSoFar = rowsSoFar;
      this.pagesSoFar = pagesSoFar;
    }

    @Override
    public CompletionStage<PageStatistics> apply(AsyncResultSet result) {
      int consumedRows = rowsSoFar;

      // Only count page if it has rows.
      int pages = result.remaining() == 0 ? pagesSoFar : pagesSoFar + 1;

      // iterate over page and ensure data is in order.
      for (Row row : result.currentPage()) {
        int v = row.getInt("v");
        if (v != consumedRows) {
          CompletableFuture<PageStatistics> next = new CompletableFuture<>();
          next.completeExceptionally(
              new Exception(String.format("Expected v == %d, got %d.", consumedRows, v)));
          return next;
        }
        consumedRows++;
      }

      if (result.hasMorePages()) {
        return result
            .fetchNextPage()
            .thenComposeAsync(new AsyncResultSetConsumingFunction(consumedRows, pages));
      } else {
        CompletableFuture<PageStatistics> next = new CompletableFuture<>();
        next.complete(new PageStatistics(consumedRows, pages));
        return next;
      }
    }
  }
}
