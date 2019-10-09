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
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe // wraps a mutable queue
public class DefaultAsyncGraphResultSet implements AsyncGraphResultSet {

  private final GraphExecutionInfo executionInfo;
  private final CountingIterator<GraphNode> iterator;
  private final Iterable<GraphNode> currentPage;

  public DefaultAsyncGraphResultSet(GraphExecutionInfo executionInfo, Queue<GraphNode> data) {
    this.executionInfo = executionInfo;
    this.iterator = new GraphResultIterator(data);
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
    // hard-coded until DSE graph supports paging
    return false;
  }

  @NonNull
  @Override
  public CompletionStage<AsyncGraphResultSet> fetchNextPage() throws IllegalStateException {
    // hard-coded until DSE graph supports paging
    throw new IllegalStateException(
        "No next page. Use #hasMorePages before calling this method to avoid this error.");
  }

  @Override
  public void cancel() {
    // nothing to do
  }

  private static class GraphResultIterator extends CountingIterator<GraphNode> {

    private final Queue<GraphNode> data;

    // Sometimes a traversal can yield the same result multiple times consecutively. To avoid
    // duplicating the data, DSE graph sends it only once with a counter indicating how many times
    // it's repeated.
    private long repeat = 0;
    private GraphNode lastGraphNode = null;

    private GraphResultIterator(Queue<GraphNode> data) {
      super(data.size());
      this.data = data;
    }

    @Override
    protected GraphNode computeNext() {
      if (repeat > 1) {
        repeat -= 1;
        // Note that we don't make a defensive copy, we assume the client won't mutate the node
        return lastGraphNode;
      }

      GraphNode container = data.poll();
      if (container == null) {
        return endOfData();
      }

      // The repeat counter is called "bulk" in the JSON payload
      GraphNode b = container.getByKey("bulk");
      if (b != null) {
        this.repeat = b.asLong();
      }

      lastGraphNode = container.getByKey("result");
      return lastGraphNode;
    }
  }
}
