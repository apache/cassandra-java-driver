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

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import net.jcip.annotations.NotThreadSafe;

/**
 * A Spliterator for {@link PagingIterable} instances that splits the stream in chunks of equal
 * size.
 *
 * @param <ElementT> The element type of the underlying stream.
 */
@NotThreadSafe
public class PagingIterableSpliterator<ElementT> implements Spliterator<ElementT> {

  @NonNull
  public static <ElementT> Builder<ElementT> builder(@NonNull PagingIterable<ElementT> iterable) {
    return new Builder<>(iterable);
  }

  /** The default chunk size for {@link PagingIterableSpliterator}. */
  public static final int DEFAULT_CHUNK_SIZE = 128;

  private final PagingIterable<ElementT> iterable;
  private long estimatedSize;
  private final int chunkSize;
  private final int characteristics;

  /**
   * Creates a new {@link PagingIterableSpliterator} for the given iterable, with unknown size and
   * default chunk size ({@value #DEFAULT_CHUNK_SIZE}).
   *
   * @param iterable The {@link PagingIterable} to create a spliterator for.
   */
  public PagingIterableSpliterator(@NonNull PagingIterable<ElementT> iterable) {
    this(iterable, Long.MAX_VALUE, DEFAULT_CHUNK_SIZE);
  }

  private PagingIterableSpliterator(
      @NonNull PagingIterable<ElementT> iterable, long estimatedSize, int chunkSize) {
    this.iterable = Objects.requireNonNull(iterable, "iterable cannot be null");
    this.estimatedSize = estimatedSize;
    this.chunkSize = chunkSize;
    if (estimatedSize < Long.MAX_VALUE) {
      characteristics =
          Spliterator.ORDERED
              | Spliterator.IMMUTABLE
              | Spliterator.NONNULL
              | Spliterator.SIZED
              | Spliterator.SUBSIZED;
    } else {
      characteristics = Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.NONNULL;
    }
  }

  @Override
  public boolean tryAdvance(Consumer<? super ElementT> action) {
    Objects.requireNonNull(action, "action cannot be null");
    ElementT row = iterable.one();
    if (row == null) {
      return false;
    }
    action.accept(row);
    return true;
  }

  @Override
  @Nullable
  public Spliterator<ElementT> trySplit() {
    if (estimatedSize != Long.MAX_VALUE && estimatedSize <= chunkSize) {
      // There is no point in splitting if the number of remaining elements is below the chunk size
      return null;
    }
    ElementT row = iterable.one();
    if (row == null) {
      return null;
    }
    Object[] array = new Object[chunkSize];
    int i = 0;
    do {
      array[i++] = row;
      if (i < chunkSize) {
        row = iterable.one();
      } else {
        break;
      }
    } while (row != null);
    if (estimatedSize != Long.MAX_VALUE) {
      estimatedSize -= i;
    }
    // Splits will also report SIZED and SUBSIZED as well.
    return Spliterators.spliterator(array, 0, i, characteristics());
  }

  @Override
  public void forEachRemaining(Consumer<? super ElementT> action) {
    iterable.iterator().forEachRemaining(action);
  }

  @Override
  public long estimateSize() {
    return estimatedSize;
  }

  @Override
  public int characteristics() {
    return characteristics;
  }

  public static class Builder<ElementT> {

    private final PagingIterable<ElementT> iterable;
    private long estimatedSize = Long.MAX_VALUE;
    private int chunkSize = DEFAULT_CHUNK_SIZE;

    Builder(@NonNull PagingIterable<ElementT> iterable) {
      this.iterable = iterable;
    }

    @NonNull
    public Builder<ElementT> withEstimatedSize(long estimatedSize) {
      Preconditions.checkArgument(estimatedSize >= 0, "estimatedSize must be >= 0");
      this.estimatedSize = estimatedSize;
      return this;
    }

    @NonNull
    public Builder<ElementT> withChunkSize(int chunkSize) {
      Preconditions.checkArgument(chunkSize > 0, "chunkSize must be > 0");
      this.chunkSize = chunkSize;
      return this;
    }

    @NonNull
    public PagingIterableSpliterator<ElementT> build() {
      return new PagingIterableSpliterator<>(iterable, estimatedSize, chunkSize);
    }
  }
}
