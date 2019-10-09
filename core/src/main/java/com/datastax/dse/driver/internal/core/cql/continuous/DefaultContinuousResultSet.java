/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.continuous;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import net.jcip.annotations.NotThreadSafe;

/**
 * This class is roughly equivalent to {@link
 * com.datastax.oss.driver.internal.core.cql.MultiPageResultSet}, except that {@link
 * RowIterator#maybeMoveToNextPage()} needs to check for cancellation before fetching the next page.
 */
@NotThreadSafe
public class DefaultContinuousResultSet implements ContinuousResultSet {

  private final RowIterator iterator;
  private final List<ExecutionInfo> executionInfos = new ArrayList<>();
  private final ColumnDefinitions columnDefinitions;

  public DefaultContinuousResultSet(ContinuousAsyncResultSet firstPage) {
    iterator = new RowIterator(firstPage);
    columnDefinitions = firstPage.getColumnDefinitions();
    executionInfos.add(firstPage.getExecutionInfo());
  }

  @Override
  public void cancel() {
    iterator.cancel();
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefinitions;
  }

  @NonNull
  @Override
  public List<ExecutionInfo> getExecutionInfos() {
    return executionInfos;
  }

  @NonNull
  @Override
  public Iterator<Row> iterator() {
    return iterator;
  }

  @Override
  public boolean isFullyFetched() {
    return iterator.isFullyFetched();
  }

  @Override
  public int getAvailableWithoutFetching() {
    return iterator.remaining();
  }

  @Override
  public boolean wasApplied() {
    return iterator.wasApplied();
  }

  private class RowIterator extends CountingIterator<Row> {
    private ContinuousAsyncResultSet currentPage;
    private Iterator<Row> currentRows;
    private boolean cancelled = false;

    private RowIterator(ContinuousAsyncResultSet firstPage) {
      super(firstPage.remaining());
      currentPage = firstPage;
      currentRows = firstPage.currentPage().iterator();
    }

    @Override
    protected Row computeNext() {
      maybeMoveToNextPage();
      return currentRows.hasNext() ? currentRows.next() : endOfData();
    }

    private void maybeMoveToNextPage() {
      if (!cancelled && !currentRows.hasNext() && currentPage.hasMorePages()) {
        BlockingOperation.checkNotDriverThread();
        ContinuousAsyncResultSet nextPage =
            CompletableFutures.getUninterruptibly(currentPage.fetchNextPage());
        currentPage = nextPage;
        remaining += currentPage.remaining();
        currentRows = nextPage.currentPage().iterator();
        executionInfos.add(nextPage.getExecutionInfo());
      }
    }

    private boolean isFullyFetched() {
      return !currentPage.hasMorePages();
    }

    private boolean wasApplied() {
      return currentPage.wasApplied();
    }

    private void cancel() {
      currentPage.cancel();
      cancelled = true;
    }
  }
}
