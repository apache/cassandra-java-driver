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
package com.datastax.oss.driver.internal.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.DefaultAsyncResultSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AsyncPagingIterableWrapperTest {

  @Mock private ColumnDefinitions columnDefinitions;
  @Mock private Statement<?> statement;
  @Mock private CqlSession session;
  @Mock private InternalDriverContext context;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    // One single column "i" of type int:
    when(columnDefinitions.contains("i")).thenReturn(true);
    ColumnDefinition iDefinition = mock(ColumnDefinition.class);
    when(iDefinition.getType()).thenReturn(DataTypes.INT);
    when(columnDefinitions.get("i")).thenReturn(iDefinition);
    when(columnDefinitions.firstIndexOf("i")).thenReturn(0);
    when(columnDefinitions.get(0)).thenReturn(iDefinition);

    when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(context.getProtocolVersion()).thenReturn(DefaultProtocolVersion.DEFAULT);
  }

  @Test
  public void should_wrap_result_set() throws Exception {
    // Given
    // two pages of data:
    ExecutionInfo executionInfo1 = mockExecutionInfo();
    DefaultAsyncResultSet resultSet1 =
        new DefaultAsyncResultSet(
            columnDefinitions, executionInfo1, mockData(0, 5), session, context);
    DefaultAsyncResultSet resultSet2 =
        new DefaultAsyncResultSet(
            columnDefinitions, mockExecutionInfo(), mockData(5, 10), session, context);
    // chain them together:
    ByteBuffer mockPagingState = ByteBuffer.allocate(0);
    when(executionInfo1.getPagingState()).thenReturn(mockPagingState);
    Statement<?> mockNextStatement = mock(Statement.class);
    when(((Statement) statement).copy(mockPagingState)).thenReturn(mockNextStatement);
    when(session.executeAsync(mockNextStatement))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(resultSet2));

    // When
    MappedAsyncPagingIterable<Integer> iterable1 = resultSet1.map(row -> row.getInt("i"));

    // Then
    for (int i = 0; i < 5; i++) {
      assertThat(iterable1.one()).isEqualTo(i);
      assertThat(iterable1.remaining()).isEqualTo(resultSet1.remaining()).isEqualTo(4 - i);
    }
    assertThat(iterable1.hasMorePages()).isTrue();

    MappedAsyncPagingIterable<Integer> iterable2 =
        iterable1.fetchNextPage().toCompletableFuture().get();
    for (int i = 5; i < 10; i++) {
      assertThat(iterable2.one()).isEqualTo(i);
      assertThat(iterable2.remaining()).isEqualTo(resultSet2.remaining()).isEqualTo(9 - i);
    }
    assertThat(iterable2.hasMorePages()).isFalse();
  }

  /** Checks that consuming from the wrapper consumes from the source, and vice-versa. */
  @Test
  public void should_share_iteration_progress_with_wrapped_result_set() {
    // Given
    DefaultAsyncResultSet resultSet =
        new DefaultAsyncResultSet(
            columnDefinitions, mockExecutionInfo(), mockData(0, 10), session, context);

    // When
    MappedAsyncPagingIterable<Integer> iterable = resultSet.map(row -> row.getInt("i"));

    // Then
    // Consume alternatively from the source and mapped iterable, and check that they stay in sync
    for (int i = 0; i < 10; i++) {
      Object element = (i % 2 == 0 ? resultSet : iterable).one();
      assertThat(element).isNotNull();
      assertThat(iterable.remaining()).isEqualTo(resultSet.remaining()).isEqualTo(9 - i);
    }
    assertThat(resultSet.hasMorePages()).isFalse();
    assertThat(iterable.hasMorePages()).isFalse();
  }

  private ExecutionInfo mockExecutionInfo() {
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(executionInfo.getRequest()).thenAnswer(invocation -> statement);
    return executionInfo;
  }

  private Queue<List<ByteBuffer>> mockData(int start, int end) {
    Queue<List<ByteBuffer>> data = new ArrayDeque<>();
    for (int i = start; i < end; i++) {
      data.add(Lists.newArrayList(TypeCodecs.INT.encode(i, DefaultProtocolVersion.DEFAULT)));
    }
    return data;
  }
}
