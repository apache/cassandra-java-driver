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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.PagingIterableWrapper;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Function;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class SinglePageResultSet implements ResultSet {
  private final AsyncResultSet onlyPage;

  public SinglePageResultSet(AsyncResultSet onlyPage) {
    this.onlyPage = onlyPage;
    assert !onlyPage.hasMorePages();
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return onlyPage.getColumnDefinitions();
  }

  @NonNull
  @Override
  public ExecutionInfo getExecutionInfo() {
    return onlyPage.getExecutionInfo();
  }

  @NonNull
  @Override
  public List<ExecutionInfo> getExecutionInfos() {
    // Assuming this will be called 0 or 1 time, avoid creating the list if it's 0.
    return ImmutableList.of(onlyPage.getExecutionInfo());
  }

  @Override
  public boolean isFullyFetched() {
    return true;
  }

  @Override
  public int getAvailableWithoutFetching() {
    return onlyPage.remaining();
  }

  @NonNull
  @Override
  public Iterator<Row> iterator() {
    return onlyPage.currentPage().iterator();
  }

  @NonNull
  @Override
  public Spliterator<Row> spliterator() {
    return PagingIterableSpliterator.builder(this)
        .withEstimatedSize(getAvailableWithoutFetching())
        .build();
  }

  @NonNull
  @Override
  public <TargetElementT> PagingIterable<TargetElementT> map(
      Function<? super Row, ? extends TargetElementT> elementMapper) {
    return new PagingIterableWrapper<>(this, elementMapper, true);
  }

  @Override
  public boolean wasApplied() {
    return onlyPage.wasApplied();
  }
}
