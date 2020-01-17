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

import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import java.util.List;

public class MockPagingIterable<T> implements PagingIterable<T> {

  private final Iterator<T> iterator;

  public MockPagingIterable(Iterator<T> iterator) {
    this.iterator = iterator;
  }

  @NonNull
  @Override
  public Iterator<T> iterator() {
    return iterator;
  }

  @Override
  public boolean isFullyFetched() {
    return !iterator.hasNext();
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    throw new UnsupportedOperationException("irrelevant");
  }

  @NonNull
  @Override
  public List<ExecutionInfo> getExecutionInfos() {
    throw new UnsupportedOperationException("irrelevant");
  }

  @Override
  public int getAvailableWithoutFetching() {
    throw new UnsupportedOperationException("irrelevant");
  }

  @Override
  public boolean wasApplied() {
    throw new UnsupportedOperationException("irrelevant");
  }
}
