/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.continuous;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe // wraps a mutable queue
public class DefaultContinuousAsyncResultSet implements ContinuousAsyncResultSet {

  private final Iterable<Row> currentPage;
  private final ColumnDefinitions columnDefinitions;
  private final int pageNumber;
  private final boolean hasMorePages;
  private final ExecutionInfo executionInfo;
  private final ContinuousCqlRequestHandler handler;
  private final CountingIterator<Row> iterator;

  public DefaultContinuousAsyncResultSet(
      CountingIterator<Row> iterator,
      ColumnDefinitions columnDefinitions,
      int pageNumber,
      boolean hasMorePages,
      ExecutionInfo executionInfo,
      ContinuousCqlRequestHandler handler) {
    this.columnDefinitions = columnDefinitions;
    this.pageNumber = pageNumber;
    this.hasMorePages = hasMorePages;
    this.executionInfo = executionInfo;
    this.handler = handler;
    this.iterator = iterator;
    this.currentPage = () -> iterator;
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefinitions;
  }

  @Override
  public boolean wasApplied() {
    // always return true for non-conditional updates
    return true;
  }

  @NonNull
  @Override
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @Override
  public int pageNumber() {
    return pageNumber;
  }

  @Override
  public boolean hasMorePages() {
    return hasMorePages;
  }

  @NonNull
  @Override
  public Iterable<Row> currentPage() {
    return currentPage;
  }

  @Override
  public int remaining() {
    return iterator.remaining();
  }

  @NonNull
  @Override
  public CompletionStage<ContinuousAsyncResultSet> fetchNextPage() throws IllegalStateException {
    if (!hasMorePages()) {
      throw new IllegalStateException(
          "Can't call fetchNextPage() on the last page (use hasMorePages() to check)");
    }
    return handler.dequeueOrCreatePending();
  }

  @Override
  public void cancel() {
    handler.cancel();
  }

  static ContinuousAsyncResultSet empty(ExecutionInfo executionInfo) {

    return new ContinuousAsyncResultSet() {

      @NonNull
      @Override
      public ColumnDefinitions getColumnDefinitions() {
        return EmptyColumnDefinitions.INSTANCE;
      }

      @NonNull
      @Override
      public ExecutionInfo getExecutionInfo() {
        return executionInfo;
      }

      @NonNull
      @Override
      public Iterable<Row> currentPage() {
        return Collections.emptyList();
      }

      @Override
      public int remaining() {
        return 0;
      }

      @Override
      public boolean hasMorePages() {
        return false;
      }

      @Override
      public int pageNumber() {
        return 1;
      }

      @NonNull
      @Override
      public CompletionStage<ContinuousAsyncResultSet> fetchNextPage()
          throws IllegalStateException {
        throw new IllegalStateException(
            "Can't call fetchNextPage() on the last page (use hasMorePages() to check)");
      }

      @Override
      public void cancel() {
        // noop
      }

      @Override
      public boolean wasApplied() {
        // always true
        return true;
      }
    };
  }
}
