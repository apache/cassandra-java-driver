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
package com.datastax.oss.driver.internal.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.cql.ResultSetTestBase;
import com.datastax.oss.driver.internal.core.cql.ResultSets;
import java.util.Iterator;
import org.junit.Test;

public class PagingIterableWrapperTest extends ResultSetTestBase {

  @Test
  public void should_wrap_result_set() {
    // Given
    AsyncResultSet page1 = mockPage(true, 0, 1, 2);
    AsyncResultSet page2 = mockPage(true, 3, 4, 5);
    AsyncResultSet page3 = mockPage(false, 6, 7, 8);

    complete(page1.fetchNextPage(), page2);
    complete(page2.fetchNextPage(), page3);

    // When
    PagingIterable<Integer> iterable = ResultSets.newInstance(page1).map(row -> row.getInt(0));

    // Then
    assertThat(iterable.getExecutionInfo()).isSameAs(page1.getExecutionInfo());
    assertThat(iterable.getExecutionInfos()).containsExactly(page1.getExecutionInfo());

    Iterator<Integer> iterator = iterable.iterator();

    assertThat(iterator.next()).isEqualTo(0);
    assertThat(iterator.next()).isEqualTo(1);
    assertThat(iterator.next()).isEqualTo(2);

    assertThat(iterator.hasNext()).isTrue();
    // This should have triggered the fetch of page2
    assertThat(iterable.getExecutionInfo()).isEqualTo(page2.getExecutionInfo());
    assertThat(iterable.getExecutionInfos())
        .containsExactly(page1.getExecutionInfo(), page2.getExecutionInfo());

    assertThat(iterator.next()).isEqualTo(3);
    assertThat(iterator.next()).isEqualTo(4);
    assertThat(iterator.next()).isEqualTo(5);

    assertThat(iterator.hasNext()).isTrue();
    // This should have triggered the fetch of page3
    assertThat(iterable.getExecutionInfo()).isEqualTo(page3.getExecutionInfo());
    assertThat(iterable.getExecutionInfos())
        .containsExactly(
            page1.getExecutionInfo(), page2.getExecutionInfo(), page3.getExecutionInfo());

    assertThat(iterator.next()).isEqualTo(6);
    assertThat(iterator.next()).isEqualTo(7);
    assertThat(iterator.next()).isEqualTo(8);
  }

  /** Checks that consuming from the wrapper consumes from the source, and vice-versa. */
  @Test
  public void should_share_iteration_progress_with_wrapped_result_set() {
    // Given
    AsyncResultSet page1 = mockPage(true, 0, 1, 2);
    AsyncResultSet page2 = mockPage(true, 3, 4, 5);
    AsyncResultSet page3 = mockPage(false, 6, 7, 8);

    complete(page1.fetchNextPage(), page2);
    complete(page2.fetchNextPage(), page3);

    // When
    ResultSet resultSet = ResultSets.newInstance(page1);
    PagingIterable<Integer> iterable = resultSet.map(row -> row.getInt(0));

    // Then
    Iterator<Row> sourceIterator = resultSet.iterator();
    Iterator<Integer> mappedIterator = iterable.iterator();

    assertThat(mappedIterator.next()).isEqualTo(0);
    assertNextRow(sourceIterator, 1);
    assertThat(mappedIterator.next()).isEqualTo(2);
    assertNextRow(sourceIterator, 3);
    assertThat(mappedIterator.next()).isEqualTo(4);
    assertNextRow(sourceIterator, 5);
    assertThat(mappedIterator.next()).isEqualTo(6);
    assertNextRow(sourceIterator, 7);
    assertThat(mappedIterator.next()).isEqualTo(8);
  }
}
