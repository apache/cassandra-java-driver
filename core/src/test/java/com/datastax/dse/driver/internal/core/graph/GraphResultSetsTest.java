/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
    assertThat(resultSet.getExecutionInfo()).isSameAs(page1.getExecutionInfo());

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

    assertThat(resultSet.getExecutionInfo()).isSameAs(page1.getExecutionInfo());
    assertThat(((MultiPageGraphResultSet) resultSet).getExecutionInfos())
        .containsExactly(page1.getExecutionInfo());

    Iterator<GraphNode> iterator = resultSet.iterator();

    assertNextRow(iterator, 0);
    assertNextRow(iterator, 1);
    assertNextRow(iterator, 2);

    assertThat(iterator.hasNext()).isTrue();
    // This should have triggered the fetch of page2
    assertThat(resultSet.getExecutionInfo()).isEqualTo(page2.getExecutionInfo());
    assertThat(((MultiPageGraphResultSet) resultSet).getExecutionInfos())
        .containsExactly(page1.getExecutionInfo(), page2.getExecutionInfo());

    assertNextRow(iterator, 3);
    assertNextRow(iterator, 4);
    assertNextRow(iterator, 5);

    assertThat(iterator.hasNext()).isTrue();
    // This should have triggered the fetch of page3
    assertThat(resultSet.getExecutionInfo()).isEqualTo(page3.getExecutionInfo());
    assertThat(((MultiPageGraphResultSet) resultSet).getExecutionInfos())
        .containsExactly(
            page1.getExecutionInfo(), page2.getExecutionInfo(), page3.getExecutionInfo());

    assertNextRow(iterator, 6);
    assertNextRow(iterator, 7);
    assertNextRow(iterator, 8);
  }
}
