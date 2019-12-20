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
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiPageGraphResultSet implements GraphResultSet {
  private final RowIterator iterator;
  private final List<ExecutionInfo> executionInfos = new ArrayList<>();

  public MultiPageGraphResultSet(AsyncGraphResultSet firstPage) {
    iterator = new RowIterator(firstPage);
    executionInfos.add(firstPage.getRequestExecutionInfo());
  }

  @Override
  public void cancel() {
    iterator.cancel();
  }

  @NonNull
  @Override
  public ExecutionInfo getRequestExecutionInfo() {
    return executionInfos.get(executionInfos.size() - 1);
  }

  @NonNull
  @Override
  @Deprecated
  public com.datastax.dse.driver.api.core.graph.GraphExecutionInfo getExecutionInfo() {
    return GraphExecutionInfoConverter.convert(getRequestExecutionInfo());
  }

  /**
   * The execution information for all the queries that have been performed so far to assemble this
   * iterable.
   *
   * <p>This will have multiple elements if the query is paged, since the driver performs blocking
   * background queries to fetch additional pages transparently as the result set is being iterated.
   */
  @NonNull
  public List<ExecutionInfo> getRequestExecutionInfos() {
    return executionInfos;
  }

  /** @deprecated use {@link #getRequestExecutionInfos()} instead. */
  @NonNull
  @Deprecated
  public List<com.datastax.dse.driver.api.core.graph.GraphExecutionInfo> getExecutionInfos() {
    return Lists.transform(executionInfos, GraphExecutionInfoConverter::convert);
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
        executionInfos.add(nextPage.getRequestExecutionInfo());
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
