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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class MultiPageResultSet implements ResultSet {

  private final RowIterator iterator;
  private final List<ExecutionInfo> executionInfos = new ArrayList<>();
  private ColumnDefinitions columnDefinitions;

  public MultiPageResultSet(@NonNull AsyncResultSet firstPage) {
    assert firstPage.hasMorePages();
    this.iterator = new RowIterator(firstPage);
    this.executionInfos.add(firstPage.getExecutionInfo());
    this.columnDefinitions = firstPage.getColumnDefinitions();
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefinitions;
  }

  @NonNull
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

  @NonNull
  @Override
  public Iterator<Row> iterator() {
    return iterator;
  }

  @Override
  public boolean wasApplied() {
    return iterator.wasApplied();
  }

  private class RowIterator extends CountingIterator<Row> {
    private AsyncResultSet currentPage;
    private Iterator<Row> currentRows;

    private RowIterator(AsyncResultSet firstPage) {
      super(firstPage.remaining());
      this.currentPage = firstPage;
      this.currentRows = firstPage.currentPage().iterator();
    }

    @Override
    protected Row computeNext() {
      maybeMoveToNextPage();
      return currentRows.hasNext() ? currentRows.next() : endOfData();
    }

    private void maybeMoveToNextPage() {
      while (!currentRows.hasNext() && currentPage.hasMorePages()) {
        BlockingOperation.checkNotDriverThread();
        AsyncResultSet nextPage =
            CompletableFutures.getUninterruptibly(currentPage.fetchNextPage());
        currentPage = nextPage;
        remaining += nextPage.remaining();
        currentRows = nextPage.currentPage().iterator();
        executionInfos.add(nextPage.getExecutionInfo());
        // The definitions can change from page to page if this result set was built from a bound
        // 'SELECT *', and the schema was altered.
        columnDefinitions = nextPage.getColumnDefinitions();
      }
    }

    private boolean isFullyFetched() {
      return !currentPage.hasMorePages();
    }

    private boolean wasApplied() {
      return currentPage.wasApplied();
    }
  }
}
