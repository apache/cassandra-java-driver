/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.continuous;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DefaultContinuousAsyncResultSetTest {

  @Mock private ColumnDefinitions columnDefinitions;
  @Mock private ExecutionInfo executionInfo;
  @Mock private ContinuousCqlRequestHandler handler;
  @Mock private CountingIterator<Row> rows;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void should_fail_to_fetch_next_page_if_last() {
    // Given
    given(executionInfo.getPagingState()).willReturn(null);
    DefaultContinuousAsyncResultSet resultSet =
        new DefaultContinuousAsyncResultSet(
            rows, columnDefinitions, 1, false, executionInfo, handler);

    // When
    boolean hasMorePages = resultSet.hasMorePages();
    ThrowingCallable nextPage = resultSet::fetchNextPage;

    // Then
    assertThat(hasMorePages).isFalse();
    assertThatThrownBy(nextPage)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Can't call fetchNextPage() on the last page");
  }

  @Test
  public void should_invoke_handler_to_fetch_next_page() {
    // Given
    CompletableFuture<ContinuousAsyncResultSet> mockResultFuture = new CompletableFuture<>();
    given(handler.dequeueOrCreatePending()).willReturn(mockResultFuture);
    DefaultContinuousAsyncResultSet resultSet =
        new DefaultContinuousAsyncResultSet(
            rows, columnDefinitions, 1, true, executionInfo, handler);

    // When
    boolean hasMorePages = resultSet.hasMorePages();
    CompletionStage<ContinuousAsyncResultSet> nextPageFuture = resultSet.fetchNextPage();

    // Then
    assertThat(hasMorePages).isTrue();
    verify(handler).dequeueOrCreatePending();
    assertThat(nextPageFuture).isEqualTo(mockResultFuture);
  }

  @Test
  public void should_invoke_handler_to_cancel() {
    // Given
    DefaultContinuousAsyncResultSet resultSet =
        new DefaultContinuousAsyncResultSet(
            rows, columnDefinitions, 1, true, executionInfo, handler);
    // When
    resultSet.cancel();

    // Then
    verify(handler).cancel();
  }

  @Test
  public void should_report_remaining_rows() {
    // Given
    given(rows.remaining()).willReturn(42);
    DefaultContinuousAsyncResultSet resultSet =
        new DefaultContinuousAsyncResultSet(
            rows, columnDefinitions, 1, true, executionInfo, handler);

    // When
    int remaining = resultSet.remaining();
    Iterable<Row> currentPage = resultSet.currentPage();

    // Then
    assertThat(remaining).isEqualTo(42);
    assertThat(currentPage.iterator()).isSameAs(rows);
  }
}
