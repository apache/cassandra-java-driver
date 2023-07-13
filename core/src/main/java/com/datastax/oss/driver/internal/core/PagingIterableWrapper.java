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
import com.datastax.oss.driver.shaded.guava.common.collect.AbstractIterator;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class PagingIterableWrapper<SourceT, TargetT> implements PagingIterable<TargetT> {

  private final PagingIterable<SourceT> source;
  private final Iterator<TargetT> iterator;

  public PagingIterableWrapper(
      PagingIterable<SourceT> source, Function<? super SourceT, ? extends TargetT> elementMapper) {
    this.source = source;
    Iterator<SourceT> sourceIterator = source.iterator();
    this.iterator =
        new AbstractIterator<TargetT>() {
          @Override
          protected TargetT computeNext() {
            return (sourceIterator.hasNext())
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

  @Override
  public Iterator<TargetT> iterator() {
    return iterator;
  }
}
