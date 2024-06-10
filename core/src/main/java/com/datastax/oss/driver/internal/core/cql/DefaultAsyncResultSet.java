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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe // wraps a mutable queue
public class DefaultAsyncResultSet implements AsyncResultSet {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAsyncResultSet.class);

  private final ColumnDefinitions definitions;
  private final ExecutionInfo executionInfo;
  private final CqlSession session;
  private final CountingIterator<Row> iterator;
  private final Iterable<Row> currentPage;

  public DefaultAsyncResultSet(
      ColumnDefinitions definitions,
      ExecutionInfo executionInfo,
      Queue<List<ByteBuffer>> data,
      CqlSession session,
      InternalDriverContext context) {
    this.definitions = definitions;
    this.executionInfo = executionInfo;
    this.session = session;
    this.iterator =
        new CountingIterator<Row>(data.size()) {
          @Override
          protected Row computeNext() {
            List<ByteBuffer> rowData = data.poll();
            return (rowData == null) ? endOfData() : new DefaultRow(definitions, rowData, context);
          }
        };
    this.currentPage = () -> iterator;
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return definitions;
  }

  @NonNull
  @Override
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @NonNull
  @Override
  public Iterable<Row> currentPage() {
    return currentPage;
  }

  @Override
  public int remaining() {
    return iterator.remaining();
  }

  @Override
  public boolean hasMorePages() {
    return executionInfo.getPagingState() != null;
  }

  @NonNull
  @Override
  public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
    ByteBuffer nextState = executionInfo.getPagingState();
    if (nextState == null) {
      throw new IllegalStateException(
          "No next page. Use #hasMorePages before calling this method to avoid this error.");
    }
    Statement<?> statement = (Statement<?>) executionInfo.getRequest();
    LOG.trace("Fetching next page for {}", statement);
    Statement<?> nextStatement = statement.copy(nextState);
    return session.executeAsync(nextStatement);
  }

  @Override
  public boolean wasApplied() {
    if (!definitions.contains("[applied]")
        || !definitions.get("[applied]").getType().equals(DataTypes.BOOLEAN)) {
      return true;
    } else if (iterator.hasNext()) {
      // Note that [applied] has the same value for all rows, so as long as we have a row we don't
      // care which one it is.
      return iterator.peek().getBoolean("[applied]");
    } else {
      // If the server provided [applied], it means there was at least one row. So if we get here it
      // means the client consumed all the rows before, we can't handle that case because we have
      // nowhere left to read the boolean from.
      throw new IllegalStateException("This method must be called before consuming all the rows");
    }
  }

  static AsyncResultSet empty(final ExecutionInfo executionInfo) {
    return new AsyncResultSet() {
      @NonNull
      @Override
      public ColumnDefinitions getColumnDefinitions() {
        return EmptyColumnDefinitions.INSTANCE;
      }

      @NonNull
      @Override
      public ExecutionInfo getExecutionInfo() {
        return executionInfo;
      }

      @NonNull
      @Override
      public Iterable<Row> currentPage() {
        return Collections.emptyList();
      }

      @Override
      public int remaining() {
        return 0;
      }

      @Override
      public boolean hasMorePages() {
        return false;
      }

      @NonNull
      @Override
      public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
        throw new IllegalStateException(
            "No next page. Use #hasMorePages before calling this method to avoid this error.");
      }

      @Override
      public boolean wasApplied() {
        return true;
      }
    };
  }
}
