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
package com.datastax.oss.driver.internal.mapper;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.internal.core.ConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.ResultSets;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Base class for generated implementations of {@link Dao}-annotated interfaces. */
public class DaoBase {

  /** The keyspace id placeholder in {@link Query#value()}. */
  public static final String KEYSPACE_ID_PLACEHOLDER = "${keyspaceId}";

  /** The table id placeholder in {@link Query#value()}. */
  public static final String TABLE_ID_PLACEHOLDER = "${tableId}";

  /** The qualified table id placeholder in {@link Query#value()}. */
  public static final String QUALIFIED_TABLE_ID_PLACEHOLDER = "${qualifiedTableId}";

  private static final CqlIdentifier APPLIED = CqlIdentifier.fromInternal("[applied]");

  protected static CompletionStage<PreparedStatement> prepare(
      SimpleStatement statement, MapperContext context) {
    if (context.getExecutionProfileName() != null) {
      statement = statement.setExecutionProfileName(context.getExecutionProfileName());
    } else if (context.getExecutionProfile() != null) {
      statement = statement.setExecutionProfile(context.getExecutionProfile());
    }
    return context.getSession().prepareAsync(statement);
  }

  /**
   * Replaces {@link #KEYSPACE_ID_PLACEHOLDER}, {@link #TABLE_ID_PLACEHOLDER} and/or {@link
   * #QUALIFIED_TABLE_ID_PLACEHOLDER} in a query string, and turns it into a statement.
   *
   * <p>This is used for {@link Query} methods.
   *
   * @param queryStringTemplate the query string to process.
   * @param context the context that contains the keyspace and table that the DAO was created with
   *     (if any).
   * @param entityHelper the helper the entity that is returned from this query, or {@code null} if
   *     the query does not return entities.
   */
  protected static SimpleStatement replaceKeyspaceAndTablePlaceholders(
      String queryStringTemplate, MapperContext context, EntityHelper<?> entityHelper) {

    CqlIdentifier keyspaceId =
        (entityHelper != null) ? entityHelper.getKeyspaceId() : context.getKeyspaceId();
    CqlIdentifier tableId =
        (entityHelper != null) ? entityHelper.getTableId() : context.getTableId();

    String queryString = queryStringTemplate;
    if (queryString.contains(KEYSPACE_ID_PLACEHOLDER)) {
      if (keyspaceId == null) {
        throw new MapperException(
            String.format(
                "Cannot substitute %s in query '%s': the DAO wasn't built with a keyspace%s",
                KEYSPACE_ID_PLACEHOLDER,
                queryStringTemplate,
                (entityHelper == null)
                    ? ""
                    : " and entity "
                        + entityHelper.getEntityClass().getSimpleName()
                        + " does not define a default keyspace"));
      } else {
        queryString = queryString.replace(KEYSPACE_ID_PLACEHOLDER, keyspaceId.asCql(false));
      }
    }

    if (queryString.contains(TABLE_ID_PLACEHOLDER)) {
      if (tableId == null) {
        throw new MapperException(
            String.format(
                "Cannot substitute %s in query '%s': the DAO wasn't built with a table",
                TABLE_ID_PLACEHOLDER, queryStringTemplate));
      } else {
        queryString = queryString.replace(TABLE_ID_PLACEHOLDER, tableId.asCql(false));
      }
    }

    if (queryString.contains(QUALIFIED_TABLE_ID_PLACEHOLDER)) {
      if (tableId == null) {
        throw new MapperException(
            String.format(
                "Cannot substitute %s in query '%s': the DAO wasn't built with a table",
                QUALIFIED_TABLE_ID_PLACEHOLDER, queryStringTemplate));
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

  public BoundStatementBuilder populateBoundStatementWithStatementAttributes(
      BoundStatementBuilder builder,
      String profileName,
      String consistencyLevel,
      String serialConsistencyLevel,
      Boolean idempotent,
      int pageSize,
      String timeout,
      String keyspace) {

    if (!profileName.isEmpty()) {
      builder = builder.setExecutionProfileName(profileName);
    }
    if (!consistencyLevel.isEmpty()) {
      builder = builder.setConsistencyLevel(getConsistencyLevelFromName(consistencyLevel));
    }
    if (!serialConsistencyLevel.isEmpty()) {
      builder =
          builder.setSerialConsistencyLevel(getConsistencyLevelFromName(serialConsistencyLevel));
    }
    if (idempotent != null) {
      builder = builder.setIdempotence(idempotent);
    }
    if (pageSize > 0) {
      builder = builder.setPageSize(pageSize);
    }
    if (!timeout.isEmpty()) {
      builder = builder.setTimeout(Duration.parse(timeout));
    }
    if (!keyspace.isEmpty()) {
      builder = builder.setRoutingKeyspace(keyspace);
    }
    return builder;
  }

  private ConsistencyLevel getConsistencyLevelFromName(String name) {
    InternalDriverContext idContext = (InternalDriverContext) context.getSession().getContext();
    ConsistencyLevelRegistry registry = idContext.getConsistencyLevelRegistry();
    return registry.codeToLevel(registry.nameToCode(name));
  }

  protected final MapperContext context;
  protected final boolean isProtocolVersionV3;

  protected DaoBase(MapperContext context) {
    this.context = context;
    this.isProtocolVersionV3 = isProtocolVersionV3(context);
  }

  protected ResultSet execute(Statement<?> statement) {
    return context.getSession().execute(statement);
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
      throw new MapperException(
          "Expected the query to return at least one row "
              + "(return type long is intended for COUNT queries)");
    }
    if (row.getColumnDefinitions().size() == 0
        || !row.getColumnDefinitions().get(0).getType().equals(DataTypes.BIGINT)) {
      throw new MapperException(
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
    return asEntity(rs.one(), entityHelper);
  }

  private <EntityT> EntityT asEntity(Row row, EntityHelper<EntityT> entityHelper) {
    return (row == null
            // Special case for INSERT IF NOT EXISTS. If the row did not exist, the query returns
            // only [applied], we want to return null to indicate there was no previous entity
            || (row.getColumnDefinitions().size() == 1
                && row.getColumnDefinitions().get(0).getName().equals(APPLIED)))
        ? null
        : entityHelper.get(row, false);
  }

  protected <EntityT> Optional<EntityT> executeAndMapToOptionalEntity(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return Optional.ofNullable(executeAndMapToSingleEntity(statement, entityHelper));
  }

  protected <EntityT> PagingIterable<EntityT> executeAndMapToEntityIterable(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return execute(statement).map(row -> entityHelper.get(row, false));
  }

  protected <EntityT> Stream<EntityT> executeAndMapToEntityStream(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return StreamSupport.stream(
        execute(statement).map(row -> entityHelper.get(row, false)).spliterator(), false);
  }

  protected CompletableFuture<AsyncResultSet> executeAsync(Statement<?> statement) {
    CompletionStage<AsyncResultSet> stage = context.getSession().executeAsync(statement);
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
    return executeAsync(statement).thenApply(rs -> asEntity(rs.one(), entityHelper));
  }

  protected <EntityT> CompletableFuture<Optional<EntityT>> executeAsyncAndMapToOptionalEntity(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return executeAsync(statement)
        .thenApply(rs -> Optional.ofNullable(asEntity(rs.one(), entityHelper)));
  }

  protected <EntityT>
      CompletableFuture<MappedAsyncPagingIterable<EntityT>> executeAsyncAndMapToEntityIterable(
          Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return executeAsync(statement).thenApply(rs -> rs.map(row -> entityHelper.get(row, false)));
  }

  protected <EntityT> CompletableFuture<Stream<EntityT>> executeAsyncAndMapToEntityStream(
      Statement<?> statement, EntityHelper<EntityT> entityHelper) {
    return executeAsync(statement)
        .thenApply(ResultSets::newInstance)
        .thenApply(rs -> StreamSupport.stream(rs.map(entityHelper::get).spliterator(), false));
  }

  protected static void throwIfProtocolVersionV3(MapperContext context) {
    if (isProtocolVersionV3(context)) {
      throw new MapperException(
          String.format(
              "You cannot use %s.%s for protocol version V3.",
              NullSavingStrategy.class.getSimpleName(), NullSavingStrategy.DO_NOT_SET.name()));
    }
  }

  protected static boolean isProtocolVersionV3(MapperContext context) {
    return context.getSession().getContext().getProtocolVersion().getCode()
        <= ProtocolConstants.Version.V3;
  }
}
