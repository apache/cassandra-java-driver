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
package com.datastax.oss.driver.internal.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public abstract class ResultSetTestBase {

  /** Mocks an async result set where column 0 has type INT, with rows with the provided data. */
  protected AsyncResultSet mockPage(boolean nextPage, Integer... data) {
    AsyncResultSet page = mock(AsyncResultSet.class);

    ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
    when(page.getColumnDefinitions()).thenReturn(columnDefinitions);

    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(page.getExecutionInfo()).thenReturn(executionInfo);

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
    CountingIterator<Row> iterator =
        new CountingIterator<Row>(queue.size()) {
          @Override
          protected Row computeNext() {
            Integer index = queue.poll();
            return (index == null) ? endOfData() : mockRow(index);
          }
        };
    when(page.currentPage()).thenReturn(() -> iterator);
    when(page.remaining()).thenAnswer(invocation -> iterator.remaining());

    return page;
  }

  private Row mockRow(int index) {
    Row row = mock(Row.class);
    when(row.getInt(0)).thenReturn(index);
    return row;
  }

  protected static void complete(CompletionStage<AsyncResultSet> stage, AsyncResultSet result) {
    stage.toCompletableFuture().complete(result);
  }

  protected void assertNextRow(Iterator<Row> iterator, int expectedValue) {
    assertThat(iterator.hasNext()).isTrue();
    Row row = iterator.next();
    assertThat(row.getInt(0)).isEqualTo(expectedValue);
  }
}
