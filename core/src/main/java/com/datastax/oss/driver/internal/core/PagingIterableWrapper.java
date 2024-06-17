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

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.cql.PagingIterableSpliterator;
import com.datastax.oss.driver.shaded.guava.common.collect.AbstractIterator;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Function;

public class PagingIterableWrapper<SourceT, TargetT> implements PagingIterable<TargetT> {

  private final PagingIterable<SourceT> source;
  private final boolean sized;
  private final Iterator<TargetT> iterator;

  /**
   * Creates a {@link PagingIterableWrapper} for the given source, with unknown size. Spliterators
   * for this iterable will never report {@link Spliterator#SIZED}.
   *
   * @param source The source to wrap.
   * @param elementMapper The element mapper.
   */
  public PagingIterableWrapper(
      @NonNull PagingIterable<SourceT> source,
      @NonNull Function<? super SourceT, ? extends TargetT> elementMapper) {
    this(source, elementMapper, false);
  }

  /**
   * Creates a {@link PagingIterableWrapper} for the given source. If {@code sized} is {@code true},
   * spliterators for this iterable will report {@link Spliterator#SIZED} and {@link
   * Spliterator#SUBSIZED} and their estimated size will be {@link #getAvailableWithoutFetching()}.
   *
   * @param source The source to wrap.
   * @param elementMapper The element mapper.
   * @param sized Whether this iterable has a known size or not.
   */
  public PagingIterableWrapper(
      @NonNull PagingIterable<SourceT> source,
      @NonNull Function<? super SourceT, ? extends TargetT> elementMapper,
      boolean sized) {
    this.source = source;
    this.sized = sized;
    Iterator<SourceT> sourceIterator = source.iterator();
    this.iterator =
        new AbstractIterator<TargetT>() {
          @Override
          protected TargetT computeNext() {
            return sourceIterator.hasNext()
                ? elementMapper.apply(sourceIterator.next())
                : endOfData();
          }
        };
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return source.getColumnDefinitions();
  }

  @NonNull
  @Override
  public List<ExecutionInfo> getExecutionInfos() {
    return source.getExecutionInfos();
  }

  @Override
  public boolean isFullyFetched() {
    return source.isFullyFetched();
  }

  @Override
  public int getAvailableWithoutFetching() {
    return source.getAvailableWithoutFetching();
  }

  @Override
  public boolean wasApplied() {
    return source.wasApplied();
  }

  @NonNull
  @Override
  public Iterator<TargetT> iterator() {
    return iterator;
  }

  @NonNull
  @Override
  public Spliterator<TargetT> spliterator() {
    PagingIterableSpliterator.Builder<TargetT> builder = PagingIterableSpliterator.builder(this);
    if (sized) {
      builder.withEstimatedSize(getAvailableWithoutFetching());
    }
    return builder.build();
  }

  @NonNull
  @Override
  public <NewTargetT> PagingIterable<NewTargetT> map(
      Function<? super TargetT, ? extends NewTargetT> elementMapper) {
    return new PagingIterableWrapper<>(this, elementMapper, sized);
  }
}
