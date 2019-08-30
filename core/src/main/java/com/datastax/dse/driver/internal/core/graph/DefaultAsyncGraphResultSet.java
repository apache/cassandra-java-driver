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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe // wraps a mutable queue
public class DefaultAsyncGraphResultSet implements AsyncGraphResultSet {

  private final ExecutionInfo executionInfo;
  private final CountingIterator<GraphNode> iterator;
  private final Iterable<GraphNode> currentPage;

  public DefaultAsyncGraphResultSet(
      ExecutionInfo executionInfo, Queue<GraphNode> data, GraphProtocol graphProtocol) {
    this.executionInfo = executionInfo;
    this.iterator = new GraphResultIterator(data, graphProtocol);
    this.currentPage = () -> iterator;
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
    return false;
  }

  @NonNull
  @Override
  public CompletionStage<AsyncGraphResultSet> fetchNextPage() throws IllegalStateException {
    throw new IllegalStateException(
        "No next page. Use #hasMorePages before calling this method to avoid this error.");
  }

  @Override
  public void cancel() {
    // nothing to do
  }
}
