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
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class MultiPageResultSet implements ResultSet {

  // Reminder: by contract this is not thread-safe, so we don't need any synchronization.

  private final RowIterator iterator;
  private final ColumnDefinitions columnDefinitions;
  private final List<ExecutionInfo> executionInfos = new ArrayList<>();

  public MultiPageResultSet(AsyncResultSet firstPage) {
    assert firstPage.hasMorePages();
    this.iterator = new RowIterator(firstPage);
    this.executionInfos.add(firstPage.getExecutionInfo());
    // This is the same for all pages
    this.columnDefinitions = firstPage.getColumnDefinitions();
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefinitions;
  }

  @Override
  public List<ExecutionInfo> getExecutionInfos() {
    return executionInfos;
  }

  @Override
  public boolean isFullyFetched() {
    return iterator.isFullyFetched();
  }

  @Override
  public int getAvailableWithoutFetching() {
    return iterator.remaining();
  }

  @Override
  public void fetchNextPage() {
    iterator.fetchNextPage();
  }

  @Override
  public Iterator<Row> iterator() {
    return iterator;
  }

  @Override
  public boolean wasApplied() {
    return iterator.wasApplied();
  }

  private class RowIterator extends CountingIterator<Row> {
    // The pages fetched so far. The first is the one we're currently iterating.
    private LinkedList<AsyncResultSet> pages = new LinkedList<>();
    private Iterator<Row> currentRows;

    private RowIterator(AsyncResultSet firstPage) {
      super(firstPage.remaining());
      this.pages.add(firstPage);
      this.currentRows = firstPage.iterator();
    }

    @Override
    protected Row computeNext() {
      maybeMoveToNextPage();
      return currentRows.hasNext() ? currentRows.next() : endOfData();
    }

    private void maybeMoveToNextPage() {
      if (!currentRows.hasNext()) {
        fetchNextPage();
        // We've just finished iterating the current page, remove it
        pages.removeFirst();
        if (!pages.isEmpty()) {
          currentRows = pages.getFirst().iterator();
        }
      }
    }

    private boolean isFullyFetched() {
      return pages.isEmpty() || !pages.getLast().hasMorePages();
    }

    private void fetchNextPage() {
      if (!pages.isEmpty()) {
        AsyncResultSet lastPage = pages.getLast();
        if (lastPage.hasMorePages()) {
          BlockingOperation.checkNotDriverThread();
          AsyncResultSet nextPage =
              CompletableFutures.getUninterruptibly(pages.getLast().fetchNextPage());
          executionInfos.add(nextPage.getExecutionInfo());
          pages.offer(nextPage);
          remaining += nextPage.remaining();
        }
      }
    }

    private boolean wasApplied() {
      if (!columnDefinitions.contains("[applied]")
          || !columnDefinitions.get("[applied]").getType().equals(DataTypes.BOOLEAN)) {
        return true;
      } else if (pages.isEmpty()) {
        throw new IllegalStateException("This method must be called before consuming all the rows");
      } else {
        return pages.getFirst().wasApplied();
      }
    }
  }
}
