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
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.shaded.guava.common.collect.AbstractIterator;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class AsyncPagingIterableWrapper<SourceT, TargetT>
    implements MappedAsyncPagingIterable<TargetT> {

  private final AsyncPagingIterable<SourceT, ?> source;
  private final Function<? super SourceT, ? extends TargetT> elementMapper;

  private final Iterable<TargetT> currentPage;

  public AsyncPagingIterableWrapper(
      AsyncPagingIterable<SourceT, ?> source,
      Function<? super SourceT, ? extends TargetT> elementMapper) {
    this.source = source;
    this.elementMapper = elementMapper;

    Iterator<SourceT> sourceIterator = source.currentPage().iterator();
    Iterator<TargetT> iterator =
        new AbstractIterator<TargetT>() {
          @Override
          protected TargetT computeNext() {
            return sourceIterator.hasNext()
                ? elementMapper.apply(sourceIterator.next())
                : endOfData();
          }
        };
    this.currentPage = () -> iterator;
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return source.getColumnDefinitions();
  }

  @NonNull
  @Override
  public ExecutionInfo getExecutionInfo() {
    return source.getExecutionInfo();
  }

  @Override
  public int remaining() {
    return source.remaining();
  }

  @NonNull
  @Override
  public Iterable<TargetT> currentPage() {
    return currentPage;
  }

  @Override
  public boolean hasMorePages() {
    return source.hasMorePages();
  }

  @NonNull
  @Override
  public CompletionStage<MappedAsyncPagingIterable<TargetT>> fetchNextPage()
      throws IllegalStateException {
    return source
        .fetchNextPage()
        .thenApply(
            nextSource ->
                new AsyncPagingIterableWrapper<SourceT, TargetT>(nextSource, elementMapper));
  }

  @Override
  public boolean wasApplied() {
    return source.wasApplied();
  }
}
