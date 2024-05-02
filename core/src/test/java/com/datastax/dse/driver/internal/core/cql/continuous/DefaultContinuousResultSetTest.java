/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.internal.core.cql.continuous;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Test;
import org.mockito.Mockito;

public class DefaultContinuousResultSetTest {

  @Test
  public void should_create_result_set_from_single_page() {
    // Given
    ContinuousAsyncResultSet page1 = mockPage(false, 0, 1, 2);

    // When
    ResultSet resultSet = new DefaultContinuousResultSet(page1);

    // Then
    assertThat(resultSet.getColumnDefinitions()).isSameAs(page1.getColumnDefinitions());
    assertThat(resultSet.getExecutionInfo()).isSameAs(page1.getExecutionInfo());
    assertThat(resultSet.getExecutionInfos()).containsExactly(page1.getExecutionInfo());

    Iterator<Row> iterator = resultSet.iterator();

    assertNextRow(iterator, 0);
    assertNextRow(iterator, 1);
    assertNextRow(iterator, 2);

    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  public void should_create_result_set_from_multiple_pages() {
    // Given
    ContinuousAsyncResultSet page1 = mockPage(true, 0, 1, 2);
    ContinuousAsyncResultSet page2 = mockPage(true, 3, 4, 5);
    ContinuousAsyncResultSet page3 = mockPage(false, 6, 7, 8);

    complete(page1.fetchNextPage(), page2);
    complete(page2.fetchNextPage(), page3);

    // When
    ResultSet resultSet = new DefaultContinuousResultSet(page1);

    // Then
    assertThat(resultSet.iterator().hasNext()).isTrue();

    assertThat(resultSet.getColumnDefinitions()).isSameAs(page1.getColumnDefinitions());
    assertThat(resultSet.getExecutionInfo()).isSameAs(page1.getExecutionInfo());
    assertThat(resultSet.getExecutionInfos()).containsExactly(page1.getExecutionInfo());

    Iterator<Row> iterator = resultSet.iterator();

    assertNextRow(iterator, 0);
    assertNextRow(iterator, 1);
    assertNextRow(iterator, 2);

    assertThat(iterator.hasNext()).isTrue();
    // This should have triggered the fetch of page2
    assertThat(resultSet.getExecutionInfo()).isEqualTo(page2.getExecutionInfo());
    assertThat(resultSet.getExecutionInfos())
        .containsExactly(page1.getExecutionInfo(), page2.getExecutionInfo());

    assertNextRow(iterator, 3);
    assertNextRow(iterator, 4);
    assertNextRow(iterator, 5);

    assertThat(iterator.hasNext()).isTrue();
    // This should have triggered the fetch of page3
    assertThat(resultSet.getExecutionInfo()).isEqualTo(page3.getExecutionInfo());
    assertThat(resultSet.getExecutionInfos())
        .containsExactly(
            page1.getExecutionInfo(), page2.getExecutionInfo(), page3.getExecutionInfo());

    assertNextRow(iterator, 6);
    assertNextRow(iterator, 7);
    assertNextRow(iterator, 8);
  }

  private static ContinuousAsyncResultSet mockPage(boolean nextPage, Integer... data) {
    ContinuousAsyncResultSet page = Mockito.mock(ContinuousAsyncResultSet.class);

    ColumnDefinitions columnDefinitions = Mockito.mock(ColumnDefinitions.class);
    Mockito.when(page.getColumnDefinitions()).thenReturn(columnDefinitions);

    ExecutionInfo executionInfo = Mockito.mock(ExecutionInfo.class);
    Mockito.when(page.getExecutionInfo()).thenReturn(executionInfo);

    if (nextPage) {
      Mockito.when(page.hasMorePages()).thenReturn(true);
      Mockito.when(page.fetchNextPage()).thenReturn(Mockito.spy(new CompletableFuture<>()));
    } else {
      Mockito.when(page.hasMorePages()).thenReturn(false);
      Mockito.when(page.fetchNextPage()).thenThrow(new IllegalStateException());
    }

    Iterator<Integer> rows = Arrays.asList(data).iterator();
    CountingIterator<Row> iterator =
        new CountingIterator<Row>(data.length) {
          @Override
          protected Row computeNext() {
            return rows.hasNext() ? mockRow(rows.next()) : endOfData();
          }
        };
    Mockito.when(page.currentPage()).thenReturn(() -> iterator);
    Mockito.when(page.remaining()).thenAnswer(invocation -> iterator.remaining());

    return page;
  }

  private static Row mockRow(int index) {
    Row row = Mockito.mock(Row.class);
    Mockito.when(row.getInt(0)).thenReturn(index);
    return row;
  }

  private static void complete(
      CompletionStage<ContinuousAsyncResultSet> stage, ContinuousAsyncResultSet result) {
    stage.toCompletableFuture().complete(result);
  }

  private static void assertNextRow(Iterator<Row> iterator, int expectedValue) {
    assertThat(iterator.hasNext()).isTrue();
    Row row0 = iterator.next();
    assertThat(row0.getInt(0)).isEqualTo(expectedValue);
  }
}
