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
package com.datastax.oss.driver.internal.mapper;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Base class for generated implementations of {@link Dao}-annotated interfaces. */
public class DaoBase {

  protected static CompletionStage<PreparedStatement> prepare(
      SimpleStatement statement, MapperContext context) {
    return context.getSession().execute(new DefaultPrepareRequest(statement), PrepareRequest.ASYNC);
  }

  protected final MapperContext context;

  protected DaoBase(MapperContext context) {
    this.context = context;
  }

  protected ResultSet execute(Statement<?> statement) {
    return context.getSession().execute(statement, Statement.SYNC);
  }

  protected boolean executeAndMapWasAppliedToBoolean(Statement<?> statement) {
    ResultSet rs = execute(statement);
    return rs.wasApplied();
  }

  protected <EntityT> EntityT executeAndMapToSingleEntity(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    ResultSet rs = execute(statement);
    Row row = rs.one();
    return (row == null) ? null : entityHelper.get(row);
  }

  protected <EntityT> PagingIterable<EntityT> executeAndMapToEntityIterable(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return execute(statement).map(entityHelper::get);
  }

  protected CompletableFuture<AsyncResultSet> executeAsync(Statement<?> statement) {
    CompletionStage<AsyncResultSet> stage =
        context.getSession().execute(statement, Statement.ASYNC);
    // We use the generic execute which allows null results, but an async processor should always
    // return a non-null stage
    assert stage != null;
    // We allow DAO interfaces to return CompletableFuture instead of CompletionStage. This method
    // returns CompletableFuture, which makes the implementation code a bit simpler to generate.
    // In practice this has no performance impact, because the default implementation of
    // toCompletableFuture in the JDK is `return this`.
    return stage.toCompletableFuture();
  }

  protected CompletableFuture<Void> executeAsyncAndMapToVoid(Statement<?> statement) {
    return executeAsync(statement).thenApply(rs -> null);
  }

  protected CompletableFuture<Boolean> executeAsyncAndMapWasAppliedToBoolean(
      Statement<?> statement) {
    return executeAsync(statement).thenApply(AsyncResultSet::wasApplied);
  }

  protected <EntityT> CompletableFuture<EntityT> executeAsyncAndMapToSingleEntity(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return executeAsync(statement)
        .thenApply(
            rs -> {
              Row row = rs.one();
              return (row == null) ? null : entityHelper.get(row);
            });
  }

  protected <EntityT>
      CompletableFuture<MappedAsyncPagingIterable<EntityT>> executeAsyncAndMapToEntityIterable(
          Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return executeAsync(statement).thenApply(rs -> rs.map(entityHelper::get));
  }
}
