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
package com.datastax.oss.driver.mapper;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.mapper.result.MapperResultProducer;
import com.datastax.oss.driver.api.mapper.result.MapperResultProducerService;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Futures;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ListenableFuture;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.SettableFuture;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class GuavaFutureProducerService implements MapperResultProducerService {

  @Override
  public Iterable<MapperResultProducer> getProducers() {
    return ImmutableList.of(
        // Note that order matters, both producers operate on ListenableFuture<Something>,
        // the most specific must come first.
        new VoidListenableFutureProducer(), new SingleEntityListenableFutureProducer());
  }

  public abstract static class ListenableFutureProducer implements MapperResultProducer {

    @Nullable
    @Override
    public ListenableFuture<?> execute(
        @NonNull Statement<?> statement,
        @NonNull MapperContext context,
        @Nullable EntityHelper<?> entityHelper) {
      SettableFuture<Object> result = SettableFuture.create();
      context
          .getSession()
          .executeAsync(statement)
          .whenComplete(
              (resultSet, error) -> {
                if (error != null) {
                  result.setException(error);
                } else {
                  result.set(convert(resultSet, entityHelper));
                }
              });
      return result;
    }

    @Nullable
    protected abstract Object convert(
        @NonNull AsyncResultSet resultSet, @Nullable EntityHelper<?> entityHelper);

    @Nullable
    @Override
    public ListenableFuture<?> wrapError(@NonNull Exception e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  public static class VoidListenableFutureProducer extends ListenableFutureProducer {

    private static final GenericType<ListenableFuture<Void>> PRODUCED_TYPE =
        new GenericType<ListenableFuture<Void>>() {};

    @Override
    public boolean canProduce(@NonNull GenericType<?> resultType) {
      return resultType.equals(PRODUCED_TYPE);
    }

    @Nullable
    @Override
    protected Object convert(
        @NonNull AsyncResultSet resultSet, @Nullable EntityHelper<?> entityHelper) {
      // ignore results
      return null;
    }
  }

  public static class SingleEntityListenableFutureProducer extends ListenableFutureProducer {

    @Override
    public boolean canProduce(@NonNull GenericType<?> resultType) {
      return resultType.getRawType().equals(ListenableFuture.class);
    }

    @Nullable
    @Override
    protected Object convert(
        @NonNull AsyncResultSet resultSet, @Nullable EntityHelper<?> entityHelper) {
      assert entityHelper != null;
      Row row = resultSet.one();
      return (row == null) ? null : entityHelper.get(row, false);
    }
  }
}
