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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Base class for generated implementations of {@link Dao}-annotated interfaces. */
public class DaoBase {

  /** The keyspace id placeholder in {@link Query#value()}. */
  public static final String KEYSPACE_ID_PLACEHOLDER = "${keyspaceId}";

  /** The table id placeholder in {@link Query#value()}. */
  public static final String TABLE_ID_PLACEHOLDER = "${tableId}";

  /** The qualified table id placeholder in {@link Query#value()}. */
  public static final String QUALIFIED_TABLE_ID_PLACEHOLDER = "${qualifiedTableId}";

  protected static CompletionStage<PreparedStatement> prepare(
      SimpleStatement statement, MapperContext context) {
    if (statement == null) {
      // Can happen, see replaceKeyspaceAndTablePlaceholders. Simply propagate null, the generated
      // methods will know how to deal with this.
      return CompletableFuture.completedFuture(null);
    } else {
      return context
          .getSession()
          .execute(new DefaultPrepareRequest(statement), PrepareRequest.ASYNC);
    }
  }

  /**
   * Replaces {@link #KEYSPACE_ID_PLACEHOLDER}, {@link #TABLE_ID_PLACEHOLDER} and/or {@link
   * #QUALIFIED_TABLE_ID_PLACEHOLDER} in a query string, and turns it into a statement.
   *
   * @param queryString the query string to process.
   * @param context the context that contains the keyspace and table that the DAO was created with
   *     (if any).
   * @param defaultEntityTableId the default table name for the entity that is returned from this
   *     query. Might be {@code null} if the query does not return entities.
   * @return the statement, or {@code null} if we don't have enough information to fill the
   *     placeholders (the generated code will throw, we know it won't try to use the statement).
   */
  protected static SimpleStatement replaceKeyspaceAndTablePlaceholders(
      String queryString, MapperContext context, CqlIdentifier defaultEntityTableId) {

    CqlIdentifier keyspaceId = context.getKeyspaceId();
    CqlIdentifier tableId = context.getTableId();
    if (tableId == null) {
      tableId = defaultEntityTableId;
    }

    if (queryString.contains(KEYSPACE_ID_PLACEHOLDER)) {
      if (keyspaceId == null) {
        return null;
      } else {
        queryString = queryString.replace(KEYSPACE_ID_PLACEHOLDER, keyspaceId.asCql(false));
      }
    }

    if (queryString.contains(TABLE_ID_PLACEHOLDER)) {
      if (tableId == null) {
        return null;
      } else {
        queryString = queryString.replace(TABLE_ID_PLACEHOLDER, tableId.asCql(false));
      }
    }

    if (queryString.contains(QUALIFIED_TABLE_ID_PLACEHOLDER)) {
      if (tableId == null) {
        return null;
      } else {
        String qualifiedId =
            (keyspaceId == null)
                ? tableId.asCql(false)
                : keyspaceId.asCql(false) + '.' + tableId.asCql(false);
        queryString = queryString.replace(QUALIFIED_TABLE_ID_PLACEHOLDER, qualifiedId);
      }
    }

    return SimpleStatement.newInstance(queryString);
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

  protected long executeAndMapFirstColumnToLong(Statement<?> statement) {
    Row row = executeAndExtractFirstRow(statement);
    return extractCount(row);
  }

  private long extractCount(Row row) {
    if (row == null) {
      throw new IllegalArgumentException(
          "Expected the query to return at least one row "
              + "(return type long is intended for COUNT queries)");
    }
    if (row.getColumnDefinitions().size() == 0
        || !row.getColumnDefinitions().get(0).getType().equals(DataTypes.BIGINT)) {
      throw new IllegalStateException(
          "Expected the query to return a column with CQL type BIGINT in first position "
              + "(return type long is intended for COUNT queries)");
    }
    return row.getLong(0);
  }

  protected Row executeAndExtractFirstRow(Statement<?> statement) {
    return execute(statement).one();
  }

  protected <EntityT> EntityT executeAndMapToSingleEntity(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    ResultSet rs = execute(statement);
    Row row = rs.one();
    return (row == null) ? null : entityHelper.get(row);
  }

  protected <EntityT> Optional<EntityT> executeAndMapToOptionalEntity(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return Optional.ofNullable(executeAndMapToSingleEntity(statement, entityHelper));
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

  protected CompletableFuture<Long> executeAsyncAndMapFirstColumnToLong(Statement<?> statement) {
    return executeAsyncAndExtractFirstRow(statement).thenApply(this::extractCount);
  }

  protected CompletableFuture<Row> executeAsyncAndExtractFirstRow(Statement<?> statement) {
    return executeAsync(statement).thenApply(AsyncResultSet::one);
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

  protected <EntityT> CompletableFuture<Optional<EntityT>> executeAsyncAndMapToOptionalEntity(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return executeAsync(statement)
        .thenApply(
            rs -> {
              Row row = rs.one();
              return (row == null) ? Optional.empty() : Optional.of(entityHelper.get(row));
            });
  }

  protected <EntityT>
      CompletableFuture<MappedAsyncPagingIterable<EntityT>> executeAsyncAndMapToEntityIterable(
          Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return executeAsync(statement).thenApply(rs -> rs.map(entityHelper::get));
  }
}
