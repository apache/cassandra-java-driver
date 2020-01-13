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
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe // wraps a mutable queue
public class ContinuousAsyncGraphResultSet implements AsyncGraphResultSet {

  private final CountingIterator<GraphNode> iterator;
  private final int pageNumber;
  private final boolean hasMorePages;
  private final ExecutionInfo executionInfo;
  private final ContinuousGraphRequestHandler continuousGraphRequestHandler;
  private final Iterable<GraphNode> currentPage;

  public ContinuousAsyncGraphResultSet(
      ExecutionInfo executionInfo,
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
    return hasMorePages;
  }

  @NonNull
  @Override
  public CompletionStage<AsyncGraphResultSet> fetchNextPage() throws IllegalStateException {
    if (!hasMorePages()) {
      throw new IllegalStateException(
          "Can't call fetchNextPage() on the last page (use hasMorePages() to check)");
    }
    return continuousGraphRequestHandler.fetchNextPage();
  }

  @Override
  public void cancel() {
    continuousGraphRequestHandler.cancel();
  }

  /** Returns the current page's number. Pages are numbered starting from 1. */
  public int pageNumber() {
    return pageNumber;
  }

  static AsyncGraphResultSet empty(ExecutionInfo executionInfo) {

    return new AsyncGraphResultSet() {

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
