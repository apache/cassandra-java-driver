/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphExecutionInfo;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiPageGraphResultSet implements GraphResultSet {
  private final RowIterator iterator;
  private final List<GraphExecutionInfo> executionInfos = new ArrayList<>();

  public MultiPageGraphResultSet(AsyncGraphResultSet firstPage) {
    iterator = new RowIterator(firstPage);
    executionInfos.add(firstPage.getExecutionInfo());
  }

  @Override
  public void cancel() {
    iterator.cancel();
  }

  @NonNull
  @Override
  public GraphExecutionInfo getExecutionInfo() {
    return executionInfos.get(executionInfos.size() - 1);
  }

  /**
   * The execution information for all the queries that have been performed so far to assemble this
   * iterable.
   *
   * <p>This will have multiple elements if the query is paged, since the driver performs blocking
   * background queries to fetch additional pages transparently as the result set is being iterated.
   */
  @NonNull
  public List<GraphExecutionInfo> getExecutionInfos() {
    return executionInfos;
  }

  @NonNull
  @Override
  public Iterator<GraphNode> iterator() {
    return iterator;
  }

  public class RowIterator extends CountingIterator<GraphNode> {
    private AsyncGraphResultSet currentPage;
    private Iterator<GraphNode> currentRows;
    private boolean cancelled = false;

    private RowIterator(AsyncGraphResultSet firstPage) {
      super(firstPage.remaining());
      currentPage = firstPage;
      currentRows = firstPage.currentPage().iterator();
    }

    @Override
    protected GraphNode computeNext() {
      maybeMoveToNextPage();
      return currentRows.hasNext() ? currentRows.next() : endOfData();
    }

    private void maybeMoveToNextPage() {
      if (!cancelled && !currentRows.hasNext() && currentPage.hasMorePages()) {
        BlockingOperation.checkNotDriverThread();
        AsyncGraphResultSet nextPage =
            CompletableFutures.getUninterruptibly(currentPage.fetchNextPage());
        currentPage = nextPage;
        remaining += currentPage.remaining();
        currentRows = nextPage.currentPage().iterator();
        executionInfos.add(nextPage.getExecutionInfo());
      }
    }

    private void cancel() {
      currentPage.cancel();
      cancelled = true;
    }

    public boolean isCancelled() {
      return cancelled;
    }
  }
}
