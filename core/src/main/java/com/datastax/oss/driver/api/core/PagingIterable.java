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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.internal.core.PagingIterableWrapper;
import com.datastax.oss.driver.internal.core.cql.PagingIterableSpliterator;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Function;

/**
 * An iterable of elements which are fetched synchronously by the driver, possibly in multiple
 * requests.
 *
 * <p>It uses asynchronous calls internally, but blocks on the results in order to provide a
 * synchronous API to its clients. If the query is paged, only the first page will be fetched
 * initially, and iteration will trigger background fetches of the next pages when necessary.
 *
 * <p>Note that this object can only be iterated once: elements are "consumed" as they are read,
 * subsequent calls to {@code iterator()} will return the same iterator instance.
 *
 * <p>Implementations of this type are <b>not thread-safe</b>. They can only be iterated by the
 * thread that invoked {@code session.execute}.
 *
 * <p>This is a generalization of {@link ResultSet}, replacing rows by an arbitrary element type.
 */
public interface PagingIterable<ElementT> extends Iterable<ElementT> {

  /** Metadata about the columns returned by the CQL request that was used to build this result. */
  @NonNull
  ColumnDefinitions getColumnDefinitions();

  /**
   * The execution information for the last query performed for this iterable.
   *
   * <p>This is a shortcut for:
   *
   * <pre>
   * getExecutionInfos().get(getExecutionInfos().size() - 1)
   * </pre>
   *
   * @see #getExecutionInfos()
   */
  @NonNull
  default ExecutionInfo getExecutionInfo() {
    List<ExecutionInfo> infos = getExecutionInfos();
    return infos.get(infos.size() - 1);
  }

  /**
   * The execution information for all the queries that have been performed so far to assemble this
   * iterable.
   *
   * <p>This will have multiple elements if the query is paged, since the driver performs blocking
   * background queries to fetch additional pages transparently as the result set is being iterated.
   */
  @NonNull
  List<ExecutionInfo> getExecutionInfos();

  /**
   * Returns the next element, or {@code null} if the iterable is exhausted.
   *
   * <p>This is convenient for queries that are known to return exactly one row, for example count
   * queries.
   */
  @Nullable
  default ElementT one() {
    Iterator<ElementT> iterator = iterator();
    return iterator.hasNext() ? iterator.next() : null;
  }

  /**
   * Returns all the remaining elements as a list; <b>not recommended for queries that return a
   * large number of elements</b>.
   *
   * <p>Contrary to {@link #iterator()} or successive calls to {@link #one()}, this method forces
   * fetching the <b>full contents</b> at once; in particular, this means that a large number of
   * background queries might have to be run, and that all the data will be held in memory locally.
   * Therefore it is crucial to only call this method for queries that are known to return a
   * reasonable number of results.
   */
  @NonNull
  @SuppressWarnings("MixedMutabilityReturnType")
  default List<ElementT> all() {
    if (!iterator().hasNext()) {
      return Collections.emptyList();
    }
    // We can't know the actual size in advance since more pages could be fetched, but we can at
    // least allocate for what we already have.
    List<ElementT> result = Lists.newArrayListWithExpectedSize(getAvailableWithoutFetching());
    Iterables.addAll(result, this);
    return result;
  }

  /**
   * Whether all pages have been fetched from the database.
   *
   * <p>If this is {@code false}, it means that more blocking background queries will be triggered
   * as iteration continues.
   */
  boolean isFullyFetched();

  /**
   * The number of elements that can be returned from this result set before a blocking background
   * query needs to be performed to retrieve more results. In other words, this is the number of
   * elements remaining in the current page.
   *
   * <p>This is useful if you use the paging state to pause the iteration and resume it later: after
   * you've retrieved the state ({@link ExecutionInfo#getPagingState()
   * getExecutionInfo().getPagingState()}), call this method and iterate the remaining elements;
   * that way you're not leaving a gap between the last element and the position you'll restart from
   * when you reinject the state in a new query.
   */
  int getAvailableWithoutFetching();

  /**
   * If the query that produced this result was a CQL conditional update, indicate whether it was
   * successfully applied.
   *
   * <p>For consistency, this method always returns {@code true} for non-conditional queries
   * (although there is no reason to call the method in that case). This is also the case for
   * conditional DDL statements ({@code CREATE KEYSPACE... IF NOT EXISTS}, {@code CREATE TABLE... IF
   * NOT EXISTS}), for which Cassandra doesn't return an {@code [applied]} column.
   *
   * <p>Note that, for versions of Cassandra strictly lower than 2.1.0-rc2, a server-side bug (<a
   * href="https://issues.apache.org/jira/browse/CASSANDRA-7337">CASSANDRA-7337</a>) causes this
   * method to always return {@code true} for batches containing conditional queries.
   */
  boolean wasApplied();

  /**
   * Creates a new instance by transforming each element of this iterable with the provided
   * function.
   *
   * <p>Note that both instances share the same underlying data: consuming elements from the
   * transformed iterable will also consume them from this object, and vice-versa.
   */
  @NonNull
  default <TargetElementT> PagingIterable<TargetElementT> map(
      Function<? super ElementT, ? extends TargetElementT> elementMapper) {
    return new PagingIterableWrapper<>(this, elementMapper);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Default spliterators created by the driver will report the following characteristics: {@link
   * Spliterator#ORDERED}, {@link Spliterator#IMMUTABLE}, {@link Spliterator#NONNULL}. Single-page
   * result sets will also report {@link Spliterator#SIZED} and {@link Spliterator#SUBSIZED}, since
   * the result set size is known.
   *
   * <p>This method should be called at most once. Spliterators share the same underlying data but
   * do not support concurrent consumption; once a spliterator for this iterable is obtained, the
   * iterable should <em>not</em> be consumed through calls to other methods such as {@link
   * #iterator()}, {@link #one()} or {@link #all()}; doing so will result in unpredictable results.
   */
  @NonNull
  @Override
  default Spliterator<ElementT> spliterator() {
    return new PagingIterableSpliterator<>(this);
  }
}
