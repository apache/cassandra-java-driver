/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class DefaultAsyncResultSetTest {

  @Mock private ColumnDefinitions columnDefinitions;
  @Mock private ExecutionInfo executionInfo;
  @Mock private Statement statement;
  @Mock private Session session;
  @Mock private InternalDriverContext context;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(executionInfo.getStatement()).thenReturn(statement);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void should_fail_to_fetch_next_page_if_last() {
    // Given
    Mockito.when(executionInfo.getPagingState()).thenReturn(null);

    // When
    DefaultAsyncResultSet resultSet =
        new DefaultAsyncResultSet(
            columnDefinitions, executionInfo, new LinkedList<>(), session, context);

    // Then
    assertThat(resultSet.hasMorePages()).isFalse();
    resultSet.fetchNextPage();
  }

  @Test
  public void should_invoke_session_to_fetch_next_page() {
    // Given
    ByteBuffer mockPagingState = ByteBuffer.allocate(0);
    Mockito.when(executionInfo.getPagingState()).thenReturn(mockPagingState);

    Statement mockNextStatement = Mockito.mock(Statement.class);
    Mockito.when(statement.copy(mockPagingState)).thenReturn(mockNextStatement);

    CompletableFuture<AsyncResultSet> mockResultFuture = new CompletableFuture<>();
    Mockito.when(session.executeAsync(Mockito.any(Statement.class))).thenReturn(mockResultFuture);

    // When
    DefaultAsyncResultSet resultSet =
        new DefaultAsyncResultSet(
            columnDefinitions, executionInfo, new LinkedList<>(), session, context);
    assertThat(resultSet.hasMorePages()).isTrue();
    CompletionStage<AsyncResultSet> nextPageFuture = resultSet.fetchNextPage();

    // Then
    Mockito.verify(statement).copy(mockPagingState);
    Mockito.verify(session).executeAsync(mockNextStatement);
    assertThat(nextPageFuture).isEqualTo(mockResultFuture);
  }
}
