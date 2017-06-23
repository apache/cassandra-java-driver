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

import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
    Mockito.when(context.codecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    Mockito.when(context.protocolVersion()).thenReturn(CoreProtocolVersion.DEFAULT);
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

  @Test
  public void should_report_applied_if_column_not_present_and_empty() {
    // Given
    Mockito.when(columnDefinitions.contains("[applied]")).thenReturn(false);

    // When
    DefaultAsyncResultSet resultSet =
        new DefaultAsyncResultSet(
            columnDefinitions, executionInfo, new LinkedList<>(), session, context);

    // Then
    assertThat(resultSet.wasApplied()).isTrue();
  }

  @Test
  public void should_report_applied_if_column_not_present_and_not_empty() {
    // Given
    Mockito.when(columnDefinitions.contains("[applied]")).thenReturn(false);
    Queue<List<ByteBuffer>> data = new LinkedList<>();
    data.add(Lists.newArrayList(Bytes.fromHexString("0xffff")));

    // When
    DefaultAsyncResultSet resultSet =
        new DefaultAsyncResultSet(columnDefinitions, executionInfo, data, session, context);

    // Then
    assertThat(resultSet.wasApplied()).isTrue();
  }

  @Test
  public void should_report_not_applied_if_column_present_and_false() {
    // Given
    Mockito.when(columnDefinitions.contains("[applied]")).thenReturn(true);
    ColumnDefinition columnDefinition = Mockito.mock(ColumnDefinition.class);
    Mockito.when(columnDefinition.getType()).thenReturn(DataTypes.BOOLEAN);
    Mockito.when(columnDefinitions.get("[applied]")).thenReturn(columnDefinition);
    Mockito.when(columnDefinitions.firstIndexOf("[applied]")).thenReturn(0);
    Mockito.when(columnDefinitions.get(0)).thenReturn(columnDefinition);

    Queue<List<ByteBuffer>> data = new LinkedList<>();
    data.add(Lists.newArrayList(TypeCodecs.BOOLEAN.encode(false, CoreProtocolVersion.DEFAULT)));

    // When
    DefaultAsyncResultSet resultSet =
        new DefaultAsyncResultSet(columnDefinitions, executionInfo, data, session, context);

    // Then
    assertThat(resultSet.wasApplied()).isFalse();
  }

  @Test
  public void should_report_not_applied_if_column_present_and_true() {
    // Given
    Mockito.when(columnDefinitions.contains("[applied]")).thenReturn(true);
    ColumnDefinition columnDefinition = Mockito.mock(ColumnDefinition.class);
    Mockito.when(columnDefinition.getType()).thenReturn(DataTypes.BOOLEAN);
    Mockito.when(columnDefinitions.get("[applied]")).thenReturn(columnDefinition);
    Mockito.when(columnDefinitions.firstIndexOf("[applied]")).thenReturn(0);
    Mockito.when(columnDefinitions.get(0)).thenReturn(columnDefinition);

    Queue<List<ByteBuffer>> data = new LinkedList<>();
    data.add(Lists.newArrayList(TypeCodecs.BOOLEAN.encode(true, CoreProtocolVersion.DEFAULT)));

    // When
    DefaultAsyncResultSet resultSet =
        new DefaultAsyncResultSet(columnDefinitions, executionInfo, data, session, context);

    // Then
    assertThat(resultSet.wasApplied()).isTrue();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void should_fail_to_report_if_applied_if_column_present_but_empty() {
    // Given
    Mockito.when(columnDefinitions.contains("[applied]")).thenReturn(true);
    ColumnDefinition columnDefinition = Mockito.mock(ColumnDefinition.class);
    Mockito.when(columnDefinition.getType()).thenReturn(DataTypes.BOOLEAN);
    Mockito.when(columnDefinitions.get("[applied]")).thenReturn(columnDefinition);

    // When
    DefaultAsyncResultSet resultSet =
        new DefaultAsyncResultSet(
            columnDefinitions, executionInfo, new LinkedList<>(), session, context);

    // Then
    resultSet.wasApplied();
  }
}
