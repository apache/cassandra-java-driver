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

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.Iterator;
import org.junit.Test;

public class ResultSetsTest extends ResultSetTestBase {

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

    Iterator<Row> iterator = resultSet.iterator();

    assertNextRow(iterator, 0);
    assertNextRow(iterator, 1);
    assertNextRow(iterator, 2);

    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  public void should_create_result_set_from_multiple_pages() {
    // Given
    AsyncResultSet page1 = mockPage(true, 0, 1, 2);
    AsyncResultSet page2 = mockPage(true, 3, 4, 5);
    AsyncResultSet page3 = mockPage(false, 6, 7, 8);

    complete(page1.fetchNextPage(), page2);
    complete(page2.fetchNextPage(), page3);

    // When
    ResultSet resultSet = ResultSets.newInstance(page1);

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
}
