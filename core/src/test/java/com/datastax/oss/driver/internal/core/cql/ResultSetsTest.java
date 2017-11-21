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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

public class ResultSetsTest {

  @Test
  public void should_create_result_set_from_single_page() {
    // Given
    AsyncResultSet page1 = mockPage(false, 0, 1, 2);

    // When
    ResultSet resultSet = ResultSets.newInstance(page1);

    // Then
    assertThat(resultSet.getColumnDefinitions()).isSameAs(page1.getColumnDefinitions());
    assertThat(resultSet.getExecutionInfo()).isSameAs(page1.getExecutionInfo());
    assertThat(resultSet.getExecutionInfos()).containsExactly(page1.getExecutionInfo());
    assertThat(resultSet.isFullyFetched()).isTrue();
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(3);

    Iterator<Row> iterator = resultSet.iterator();

    assertNextRow(iterator, 0);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(2);

    assertNextRow(iterator, 1);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(1);

    assertNextRow(iterator, 2);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(0);

    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  public void should_create_result_set_from_multiple_pages() {
    // Given
    AsyncResultSet page1 = mockPage(true, 0, 1, 2);
    AsyncResultSet page2 = mockPage(true, 3, 4, 5);
    AsyncResultSet page3 = mockPage(false, 6, 7, 8);

    ((CompletableFuture<AsyncResultSet>) page1.fetchNextPage()).complete(page2);
    ((CompletableFuture<AsyncResultSet>) page2.fetchNextPage()).complete(page3);

    // When
    ResultSet resultSet = ResultSets.newInstance(page1);

    // Then
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(3);
    assertThat(resultSet.iterator().hasNext()).isTrue();

    assertThat(resultSet.getColumnDefinitions()).isSameAs(page1.getColumnDefinitions());
    assertThat(resultSet.getExecutionInfo()).isSameAs(page1.getExecutionInfo());
    assertThat(resultSet.getExecutionInfos()).containsExactly(page1.getExecutionInfo());
    assertThat(resultSet.isFullyFetched()).isFalse();
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(3);

    Iterator<Row> iterator = resultSet.iterator();

    assertNextRow(iterator, 0);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(2);

    assertNextRow(iterator, 1);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(1);

    assertNextRow(iterator, 2);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(0);

    assertThat(iterator.hasNext()).isTrue();
    // This should have triggered the fetch of page2
    assertThat(resultSet.getExecutionInfo()).isEqualTo(page2.getExecutionInfo());
    assertThat(resultSet.getExecutionInfos())
        .containsExactly(page1.getExecutionInfo(), page2.getExecutionInfo());
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(3);

    assertNextRow(iterator, 3);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(2);

    assertNextRow(iterator, 4);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(1);

    assertNextRow(iterator, 5);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(0);

    assertThat(iterator.hasNext()).isTrue();
    // This should have triggered the fetch of page3
    assertThat(resultSet.getExecutionInfo()).isEqualTo(page3.getExecutionInfo());
    assertThat(resultSet.getExecutionInfos())
        .containsExactly(
            page1.getExecutionInfo(), page2.getExecutionInfo(), page3.getExecutionInfo());
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(3);

    assertNextRow(iterator, 6);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(2);

    assertNextRow(iterator, 7);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(1);

    assertNextRow(iterator, 8);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(0);
  }

  @Test
  public void should_fetch_multiple_pages_in_advance() {
    // Given
    AsyncResultSet page1 = mockPage(true, 0, 1, 2);
    AsyncResultSet page2 = mockPage(true, 3, 4, 5);
    AsyncResultSet page3 = mockPage(false, 6, 7, 8);

    ((CompletableFuture<AsyncResultSet>) page1.fetchNextPage()).complete(page2);
    ((CompletableFuture<AsyncResultSet>) page2.fetchNextPage()).complete(page3);

    // When
    ResultSet resultSet = ResultSets.newInstance(page1);
    resultSet.fetchNextPage();

    // Then
    assertThat(resultSet.getExecutionInfo()).isEqualTo(page2.getExecutionInfo());
    assertThat(resultSet.getExecutionInfos())
        .containsExactly(page1.getExecutionInfo(), page2.getExecutionInfo());
    assertThat(resultSet.iterator().hasNext()).isTrue();
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(6);

    // When
    resultSet.fetchNextPage();

    // Then
    assertThat(resultSet.getExecutionInfo()).isEqualTo(page3.getExecutionInfo());
    assertThat(resultSet.getExecutionInfos())
        .containsExactly(
            page1.getExecutionInfo(), page2.getExecutionInfo(), page3.getExecutionInfo());
    assertThat(resultSet.iterator().hasNext()).isTrue();
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(9);

    Iterator<Row> iterator = resultSet.iterator();

    assertNextRow(iterator, 0);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(8);
    assertNextRow(iterator, 1);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(7);
    assertNextRow(iterator, 2);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(6);
    assertNextRow(iterator, 3);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(5);
    assertNextRow(iterator, 4);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(4);
    assertNextRow(iterator, 5);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(3);
    assertNextRow(iterator, 6);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(2);
    assertNextRow(iterator, 7);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(1);
    assertNextRow(iterator, 8);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(0);
  }

  private void assertNextRow(Iterator<Row> iterator, int expectedValue) {
    assertThat(iterator.hasNext()).isTrue();
    Row row0 = iterator.next();
    assertThat(row0.getInt(0)).isEqualTo(expectedValue);
  }

  private AsyncResultSet mockPage(boolean nextPage, Integer... data) {
    AsyncResultSet page = Mockito.mock(AsyncResultSet.class);

    ColumnDefinitions columnDefinitions = Mockito.mock(ColumnDefinitions.class);
    Mockito.when(page.getColumnDefinitions()).thenReturn(columnDefinitions);

    ExecutionInfo executionInfo = Mockito.mock(ExecutionInfo.class);
    Mockito.when(page.getExecutionInfo()).thenReturn(executionInfo);

    if (nextPage) {
      Mockito.when(page.hasMorePages()).thenReturn(true);
      CompletableFuture<AsyncResultSet> nextPageFuture = Mockito.spy(new CompletableFuture<>());
      Mockito.when(page.fetchNextPage()).thenReturn(nextPageFuture);
    } else {
      Mockito.when(page.hasMorePages()).thenReturn(false);
      Mockito.when(page.fetchNextPage()).thenThrow(new IllegalStateException());
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
    Mockito.when(page.currentPage()).thenReturn(() -> iterator);
    Mockito.when(page.remaining()).thenAnswer(invocation -> iterator.remaining());

    return page;
  }

  private Row mockRow(int index) {
    Row row = Mockito.mock(Row.class);
    Mockito.when(row.getInt(0)).thenReturn(index);
    return row;
  }
}
