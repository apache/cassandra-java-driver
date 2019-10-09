/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

/**
 * The result of an asynchronous graph query.
 *
 * <p>The default implementation returned by the driver is <b>not</b> thread-safe: the iterable
 * returned by {@link #currentPage()} should only be iterated by a single thread. However, if
 * subsequent pages are requested via {@link #fetchNextPage()}, it's safe to process those new
 * instances in other threads (as long as each individual page of results is not accessed
 * concurrently).
 *
 * @see GraphResultSet
 */
public interface AsyncGraphResultSet {

  /** The execution information for this page of results. */
  @NonNull
  GraphExecutionInfo getExecutionInfo();

  /** How many rows are left before the current page is exhausted. */
  int remaining();

  /**
   * The nodes in the current page. To keep iterating beyond that, use {@link #hasMorePages()} and
   * {@link #fetchNextPage()}.
   *
   * <p>Note that this method always returns the same object, and that that object can only be
   * iterated once: nodes are "consumed" as they are read.
   */
  @NonNull
  Iterable<GraphNode> currentPage();

  /**
   * Returns the next node, or {@code null} if the result set is exhausted.
   *
   * <p>This is convenient for queries that are known to return exactly one node.
   */
  @Nullable
  default GraphNode one() {
    Iterator<GraphNode> iterator = currentPage().iterator();
    return iterator.hasNext() ? iterator.next() : null;
  }

  /**
   * Whether there are more pages of results. If so, call {@link #fetchNextPage()} to fetch the next
   * one asynchronously.
   */
  boolean hasMorePages();

  /**
   * Fetch the next page of results asynchronously.
   *
   * @throws IllegalStateException if there are no more pages. Use {@link #hasMorePages()} to check
   *     if you can call this method.
   */
  @NonNull
  CompletionStage<AsyncGraphResultSet> fetchNextPage() throws IllegalStateException;

  /**
   * Cancels the query and asks the server to stop sending results.
   *
   * <p>At this time, graph queries are not paginated and the server sends all the results at once;
   * therefore this method has no effect.
   */
  void cancel();
}
