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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.NotThreadSafe;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

@NotThreadSafe // wraps a mutable queue
public class DefaultAsyncGraphResultSet implements AsyncGraphResultSet {

  private final ExecutionInfo executionInfo;
  private final CountingIterator<GraphNode> iterator;
  private final Iterable<GraphNode> currentPage;
  private final GraphProtocol graphProtocol;

  public DefaultAsyncGraphResultSet(
      ExecutionInfo executionInfo, Queue<GraphNode> data, GraphProtocol graphProtocol) {
    this.executionInfo = executionInfo;
    this.iterator = new GraphResultIterator(data);
    this.currentPage = () -> iterator;
    this.graphProtocol = graphProtocol;
  }

  @NonNull
  @Override
  public ExecutionInfo getRequestExecutionInfo() {
    return executionInfo;
  }

  @NonNull
  @Override
  @Deprecated
  public com.datastax.dse.driver.api.core.graph.GraphExecutionInfo getExecutionInfo() {
    return GraphExecutionInfoConverter.convert(executionInfo);
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

  private class GraphResultIterator extends CountingIterator<GraphNode> {

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

      if (graphProtocol.isGraphBinary()) {
        // results are contained in a Traverser object and not a Map if the protocol
        // is GraphBinary
        Preconditions.checkState(
            container.as(Object.class) instanceof Traverser,
            "Graph protocol error. Received object should be a Traverser but it is not.");
        Traverser t = container.as(Traverser.class);
        this.repeat = t.bulk();
        this.lastGraphNode = new ObjectGraphNode(t.get());
        return lastGraphNode;
      } else {
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
}
