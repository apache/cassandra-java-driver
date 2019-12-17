/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public abstract class GraphResultSetTestBase {

  /** Mocks an async result set where column 0 has type INT, with rows with the provided data. */
  protected AsyncGraphResultSet mockPage(boolean nextPage, Integer... data) {
    AsyncGraphResultSet page = mock(AsyncGraphResultSet.class);

    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(page.getRequestExecutionInfo()).thenReturn(executionInfo);

    if (nextPage) {
      when(page.hasMorePages()).thenReturn(true);
      when(page.fetchNextPage()).thenReturn(spy(new CompletableFuture<>()));
    } else {
      when(page.hasMorePages()).thenReturn(false);
      when(page.fetchNextPage()).thenThrow(new IllegalStateException());
    }

    // Emulate DefaultAsyncResultSet's internals (this is a bit sketchy, maybe it would be better
    // to use real DefaultAsyncResultSet instances)
    Queue<Integer> queue = Lists.newLinkedList(Arrays.asList(data));
    CountingIterator<GraphNode> iterator =
        new CountingIterator<GraphNode>(queue.size()) {
          @Override
          protected GraphNode computeNext() {
            Integer index = queue.poll();
            return (index == null) ? endOfData() : mockRow(index);
          }
        };
    when(page.currentPage()).thenReturn(() -> iterator);
    when(page.remaining()).thenAnswer(invocation -> iterator.remaining());

    return page;
  }

  private GraphNode mockRow(int index) {
    GraphNode row = mock(GraphNode.class);
    when(row.asInt()).thenReturn(index);
    return row;
  }

  protected static void complete(
      CompletionStage<AsyncGraphResultSet> stage, AsyncGraphResultSet result) {
    stage.toCompletableFuture().complete(result);
  }

  protected void assertNextRow(Iterator<GraphNode> iterator, int expectedValue) {
    assertThat(iterator.hasNext()).isTrue();
    GraphNode row = iterator.next();
    assertThat(row.asInt()).isEqualTo(expectedValue);
  }
}
