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
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe // wraps a mutable queue
public class ContinuousAsyncGraphResultSet implements AsyncGraphResultSet {

  private final CountingIterator<GraphNode> iterator;
  private final int pageNumber;
  private final boolean hasMorePages;
  private final GraphExecutionInfo executionInfo;
  private final ContinuousGraphRequestHandler continuousGraphRequestHandler;
  private final Iterable<GraphNode> currentPage;

  public ContinuousAsyncGraphResultSet(
      GraphExecutionInfo executionInfo,
      Queue<GraphNode> data,
      int pageNumber,
      boolean hasMorePages,
      ContinuousGraphRequestHandler continuousGraphRequestHandler,
      GraphProtocol graphProtocol) {

    this.iterator = new GraphResultIterator(data, graphProtocol);
    this.pageNumber = pageNumber;
    this.hasMorePages = hasMorePages;
    this.executionInfo = executionInfo;
    this.continuousGraphRequestHandler = continuousGraphRequestHandler;
    this.currentPage = () -> iterator;
  }

  @NonNull
  @Override
  public GraphExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @Override
  public int remaining() {
    return iterator.remaining();
  }

  @NonNull
  @Override
  public Iterable<GraphNode> currentPage() {
    return currentPage;
  }

  @Override
  public boolean hasMorePages() {
    return hasMorePages;
  }

  @NonNull
  @Override
  public CompletionStage<AsyncGraphResultSet> fetchNextPage() throws IllegalStateException {
    if (!hasMorePages()) {
      throw new IllegalStateException(
          "Can't call fetchNextPage() on the last page (use hasMorePages() to check)");
    }
    return continuousGraphRequestHandler.dequeueOrCreatePending();
  }

  @Override
  public void cancel() {
    continuousGraphRequestHandler.cancel();
  }

  /** Returns the current page's number. Pages are numbered starting from 1. */
  public int pageNumber() {
    return pageNumber;
  }

  static AsyncGraphResultSet empty(GraphExecutionInfo executionInfo) {

    return new AsyncGraphResultSet() {

      @NonNull
      @Override
      public GraphExecutionInfo getExecutionInfo() {
        return executionInfo;
      }

      @NonNull
      @Override
      public Iterable<GraphNode> currentPage() {
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

      @NonNull
      @Override
      public CompletionStage<AsyncGraphResultSet> fetchNextPage() throws IllegalStateException {
        throw new IllegalStateException(
            "Can't call fetchNextPage() on the last page (use hasMorePages() to check)");
      }

      @Override
      public void cancel() {
        // noop
      }
    };
  }
}
