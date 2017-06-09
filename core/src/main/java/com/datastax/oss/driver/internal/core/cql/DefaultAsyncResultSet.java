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
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionStage;

public class DefaultAsyncResultSet implements AsyncResultSet {

  private final ColumnDefinitions definitions;
  private final ExecutionInfo executionInfo;
  private final CountingIterator<Row> iterator;
  private final InternalDriverContext context;

  public DefaultAsyncResultSet(
      ColumnDefinitions definitions,
      ExecutionInfo executionInfo,
      Queue<List<ByteBuffer>> data,
      InternalDriverContext context) {
    this.definitions = definitions;
    this.executionInfo = executionInfo;
    this.iterator =
        new CountingIterator<Row>(data.size()) {
          @Override
          protected Row computeNext() {
            List<ByteBuffer> rowData = data.poll();
            return (rowData == null) ? endOfData() : new DefaultRow(definitions, rowData, context);
          }
        };
    this.context = context;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return definitions;
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @Override
  public Iterator<Row> iterator() {
    return iterator;
  }

  @Override
  public int remaining() {
    return iterator.remaining();
  }

  @Override
  public boolean hasMorePages() {
    return false;
  }

  @Override
  public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
    throw new UnsupportedOperationException("TODO implement paging");
  }

  static AsyncResultSet empty(final ExecutionInfo executionInfo) {
    return new AsyncResultSet() {
      @Override
      public ColumnDefinitions getColumnDefinitions() {
        return DefaultColumnDefinitions.EMPTY;
      }

      @Override
      public ExecutionInfo getExecutionInfo() {
        return executionInfo;
      }

      @Override
      public int remaining() {
        return 0;
      }

      @Override
      public boolean hasMorePages() {
        return false;
      }

      @Override
      public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
        throw new IllegalStateException("Empty result set has no next page");
      }

      @Override
      public Iterator<Row> iterator() {
        return Collections.<Row>emptyList().iterator();
      }
    };
  }
}
