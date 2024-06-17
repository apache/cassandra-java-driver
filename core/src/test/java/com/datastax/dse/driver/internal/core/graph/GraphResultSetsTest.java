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
package com.datastax.dse.driver.internal.core.graph;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import java.util.Iterator;
import org.junit.Test;

public class GraphResultSetsTest extends GraphResultSetTestBase {

  @Test
  public void should_create_result_set_from_single_page() {
    // Given
    AsyncGraphResultSet page1 = mockPage(false, 0, 1, 2);

    // When
    GraphResultSet resultSet = GraphResultSets.toSync(page1);

    // Then
    assertThat(resultSet.getRequestExecutionInfo()).isSameAs(page1.getRequestExecutionInfo());

    Iterator<GraphNode> iterator = resultSet.iterator();

    assertNextRow(iterator, 0);
    assertNextRow(iterator, 1);
    assertNextRow(iterator, 2);

    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  public void should_create_result_set_from_multiple_pages() {
    // Given
    AsyncGraphResultSet page1 = mockPage(true, 0, 1, 2);
    AsyncGraphResultSet page2 = mockPage(true, 3, 4, 5);
    AsyncGraphResultSet page3 = mockPage(false, 6, 7, 8);

    complete(page1.fetchNextPage(), page2);
    complete(page2.fetchNextPage(), page3);

    // When
    GraphResultSet resultSet = GraphResultSets.toSync(page1);

    // Then
    assertThat(resultSet.iterator().hasNext()).isTrue();

    assertThat(resultSet.getRequestExecutionInfo()).isSameAs(page1.getRequestExecutionInfo());
    assertThat(((MultiPageGraphResultSet) resultSet).getRequestExecutionInfos())
        .containsExactly(page1.getRequestExecutionInfo());

    Iterator<GraphNode> iterator = resultSet.iterator();

    assertNextRow(iterator, 0);
    assertNextRow(iterator, 1);
    assertNextRow(iterator, 2);

    assertThat(iterator.hasNext()).isTrue();
    // This should have triggered the fetch of page2
    assertThat(resultSet.getRequestExecutionInfo()).isEqualTo(page2.getRequestExecutionInfo());
    assertThat(((MultiPageGraphResultSet) resultSet).getRequestExecutionInfos())
        .containsExactly(page1.getRequestExecutionInfo(), page2.getRequestExecutionInfo());

    assertNextRow(iterator, 3);
    assertNextRow(iterator, 4);
    assertNextRow(iterator, 5);

    assertThat(iterator.hasNext()).isTrue();
    // This should have triggered the fetch of page3
    assertThat(resultSet.getRequestExecutionInfo()).isEqualTo(page3.getRequestExecutionInfo());
    assertThat(((MultiPageGraphResultSet) resultSet).getRequestExecutionInfos())
        .containsExactly(
            page1.getRequestExecutionInfo(),
            page2.getRequestExecutionInfo(),
            page3.getRequestExecutionInfo());

    assertNextRow(iterator, 6);
    assertNextRow(iterator, 7);
    assertNextRow(iterator, 8);
  }
}
