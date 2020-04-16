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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MockAsyncPagingIterable<ElementT>
    implements AsyncPagingIterable<ElementT, MockAsyncPagingIterable<ElementT>> {

  private final Queue<ElementT> currentPage;
  private final MockAsyncPagingIterable<ElementT> nextPage;

  public MockAsyncPagingIterable(List<ElementT> elements, int fetchSize, boolean addEmptyLastPage) {
    if (elements.size() <= fetchSize) {
      currentPage = new ArrayDeque<>(elements);
      nextPage =
          addEmptyLastPage
              ? new MockAsyncPagingIterable<>(Collections.emptyList(), fetchSize, false)
              : null;
    } else {
      currentPage = new ArrayDeque<>(elements.subList(0, fetchSize));
      nextPage =
          new MockAsyncPagingIterable<>(
              elements.subList(fetchSize, elements.size()), fetchSize, addEmptyLastPage);
    }
  }

  @NonNull
  @Override
  public Iterable<ElementT> currentPage() {
    return currentPage;
  }

  @Override
  public int remaining() {
    return currentPage.size();
  }

  @Override
  public boolean hasMorePages() {
    return nextPage != null;
  }

  @NonNull
  @Override
  public CompletionStage<MockAsyncPagingIterable<ElementT>> fetchNextPage()
      throws IllegalStateException {
    Preconditions.checkState(nextPage != null);
    return CompletableFuture.completedFuture(nextPage);
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    throw new UnsupportedOperationException("irrelevant");
  }

  @NonNull
  @Override
  public ExecutionInfo getExecutionInfo() {
    throw new UnsupportedOperationException("irrelevant");
  }

  @Override
  public boolean wasApplied() {
    throw new UnsupportedOperationException("irrelevant");
  }
}
