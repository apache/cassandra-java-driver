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
package com.datastax.oss.driver.api.querybuilder;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.querybuilder.schema.AlterKeyspaceStart;
import com.datastax.oss.driver.api.querybuilder.schema.AlterMaterializedViewStart;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTypeStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateAggregateStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateFunctionStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndexStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedViewStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTypeStart;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.schema.RelationOptions;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.LeveledCompactionStrategy;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.SizeTieredCompactionStrategy;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.TimeWindowCompactionStrategy;
import com.datastax.oss.driver.internal.core.metadata.schema.ShallowUserDefinedType;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultAlterKeyspace;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultAlterMaterializedView;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultAlterTable;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultAlterType;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateAggregate;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateFunction;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateIndex;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateKeyspace;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateMaterializedView;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateTable;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateType;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultDrop;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultDropKeyspace;
import com.datastax.oss.driver.internal.querybuilder.schema.compaction.DefaultLeveledCompactionStrategy;
import com.datastax.oss.driver.internal.querybuilder.schema.compaction.DefaultSizeTieredCompactionStrategy;
import com.datastax.oss.driver.internal.querybuilder.schema.compaction.DefaultTimeWindowCompactionStrategy;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/** A Domain-Specific Language to build CQL DDL queries using Java code. */
public class SchemaBuilder {

  /** Starts a CREATE KEYSPACE query. */
  @NonNull
  public static CreateKeyspaceStart createKeyspace(@NonNull CqlIdentifier keyspaceName) {
    return new DefaultCreateKeyspace(keyspaceName);
  }

  /**
   * Shortcut for {@link #createKeyspace(CqlIdentifier)
   * createKeyspace(CqlIdentifier.fromCql(keyspaceName))}
   */
  @NonNull
  public static CreateKeyspaceStart createKeyspace(@NonNull String keyspaceName) {
    return createKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /** Starts an ALTER KEYSPACE query. */
  @NonNull
  public static AlterKeyspaceStart alterKeyspace(@NonNull CqlIdentifier keyspaceName) {
    return new DefaultAlterKeyspace(keyspaceName);
  }

  /**
   * Shortcut for {@link #alterKeyspace(CqlIdentifier)
   * alterKeyspace(CqlIdentifier.fromCql(keyspaceName)}.
   */
  @NonNull
  public static AlterKeyspaceStart alterKeyspace(@NonNull String keyspaceName) {
    return alterKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /** Starts a DROP KEYSPACE query. */
  @NonNull
  public static Drop dropKeyspace(@NonNull CqlIdentifier keyspaceName) {
    return new DefaultDropKeyspace(keyspaceName);
  }

  /**
   * Shortcut for {@link #dropKeyspace(CqlIdentifier)
   * dropKeyspace(CqlIdentifier.fromCql(keyspaceName)}.
   */
  @NonNull
  public static Drop dropKeyspace(@NonNull String keyspaceName) {
    return dropKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /**
   * Starts a CREATE TABLE query with the given table name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   */
  @NonNull
  public static CreateTableStart createTable(@NonNull CqlIdentifier tableName) {
    return new DefaultCreateTable(tableName);
  }

  /** Starts a CREATE TABLE query with the given table name for the given keyspace name. */
  @NonNull
  public static CreateTableStart createTable(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier tableName) {
    return new DefaultCreateTable(keyspace, tableName);
  }

  /**
   * Shortcut for {@link #createTable(CqlIdentifier) createTable(CqlIdentifier.fromCql(tableName)}
   */
  @NonNull
  public static CreateTableStart createTable(@NonNull String tableName) {
    return createTable(CqlIdentifier.fromCql(tableName));
  }

  /**
   * Shortcut for {@link #createTable(CqlIdentifier,CqlIdentifier)
   * createTable(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(tableName)}
   */
  @NonNull
  public static CreateTableStart createTable(@Nullable String keyspace, @NonNull String tableName) {
    return createTable(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(tableName));
  }

  /**
   * Starts an ALTER TABLE query with the given table name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   */
  @NonNull
  public static AlterTableStart alterTable(@NonNull CqlIdentifier tableName) {
    return new DefaultAlterTable(tableName);
  }

  /** Starts an ALTER TABLE query with the given table name for the given keyspace name. */
  @NonNull
  public static AlterTableStart alterTable(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier tableName) {
    return new DefaultAlterTable(keyspace, tableName);
  }

  /** Shortcut for {@link #alterTable(CqlIdentifier) alterTable(CqlIdentifier.fromCql(tableName)} */
  @NonNull
  public static AlterTableStart alterTable(@NonNull String tableName) {
    return alterTable(CqlIdentifier.fromCql(tableName));
  }

  /**
   * Shortcut for {@link #alterTable(CqlIdentifier,CqlIdentifier)
   * alterTable(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(tableName)}
   */
  @NonNull
  public static AlterTableStart alterTable(@Nullable String keyspace, @NonNull String tableName) {
    return alterTable(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(tableName));
  }

  /**
   * Starts a DROP TABLE query. This assumes the keyspace name is already qualified for the Session
   * or Statement.
   */
  @NonNull
  public static Drop dropTable(@NonNull CqlIdentifier tableName) {
    return new DefaultDrop(tableName, "TABLE");
  }

  /** Shortcut for {@link #dropTable(CqlIdentifier) dropTable(CqlIdentifier.fromCql(tableName)}. */
  @NonNull
  public static Drop dropTable(@NonNull String tableName) {
    return dropTable(CqlIdentifier.fromCql(tableName));
  }

  /** Starts a DROP TABLE query for the given table name for the given keyspace name. */
  @NonNull
  public static Drop dropTable(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier tableName) {
    return new DefaultDrop(keyspace, tableName, "TABLE");
  }

  /**
   * Shortcut for {@link #dropTable(CqlIdentifier,CqlIdentifier)
   * dropTable(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(tableName)}.
   */
  @NonNull
  public static Drop dropTable(@Nullable String keyspace, @NonNull String tableName) {
    return dropTable(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(tableName));
  }

  /**
   * Starts a CREATE MATERIALIZED VIEW query with the given view name. This assumes the keyspace
   * name is already qualified for the Session or Statement.
   */
  @NonNull
  public static CreateMaterializedViewStart createMaterializedView(
      @NonNull CqlIdentifier viewName) {
    return new DefaultCreateMaterializedView(viewName);
  }

  /**
   * Starts a CREATE MATERIALIZED VIEW query with the given view name for the given keyspace name.
   */
  @NonNull
  public static CreateMaterializedViewStart createMaterializedView(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier viewName) {
    return new DefaultCreateMaterializedView(keyspace, viewName);
  }

  /**
   * Shortcut for {@link #createMaterializedView(CqlIdentifier)
   * createMaterializedView(CqlIdentifier.fromCql(viewName)}
   */
  @NonNull
  public static CreateMaterializedViewStart createMaterializedView(@NonNull String viewName) {
    return createMaterializedView(CqlIdentifier.fromCql(viewName));
  }

  /**
   * Shortcut for {@link #createMaterializedView(CqlIdentifier,CqlIdentifier)
   * createMaterializedView(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(viewName)}
   */
  @NonNull
  public static CreateMaterializedViewStart createMaterializedView(
      @Nullable String keyspace, @NonNull String viewName) {
    return createMaterializedView(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(viewName));
  }

  /**
   * Starts an ALTER MATERIALIZED VIEW query with the given view name. This assumes the keyspace
   * name is already qualified for the Session or Statement.
   */
  @NonNull
  public static AlterMaterializedViewStart alterMaterializedView(@NonNull CqlIdentifier viewName) {
    return new DefaultAlterMaterializedView(viewName);
  }

  /**
   * Starts an ALTER MATERIALIZED VIEW query with the given view name for the given keyspace name.
   */
  @NonNull
  public static AlterMaterializedViewStart alterMaterializedView(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier viewName) {
    return new DefaultAlterMaterializedView(keyspace, viewName);
  }

  /**
   * Shortcut for {@link #alterMaterializedView(CqlIdentifier)
   * alterMaterializedView(CqlIdentifier.fromCql(viewName)}
   */
  @NonNull
  public static AlterMaterializedViewStart alterMaterializedView(@NonNull String viewName) {
    return alterMaterializedView(CqlIdentifier.fromCql(viewName));
  }

  /**
   * Shortcut for {@link #alterMaterializedView(CqlIdentifier,CqlIdentifier)
   * alterMaterializedView(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(viewName)}
   */
  @NonNull
  public static AlterMaterializedViewStart alterMaterializedView(
      @Nullable String keyspace, @NonNull String viewName) {
    return alterMaterializedView(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(viewName));
  }

  /**
   * Starts a DROP MATERIALIZED VIEW query. This assumes the keyspace name is already qualified for
   * the Session or Statement.
   */
  @NonNull
  public static Drop dropMaterializedView(@NonNull CqlIdentifier viewName) {
    return new DefaultDrop(viewName, "MATERIALIZED VIEW");
  }

  /**
   * Shortcut for {@link #dropMaterializedView(CqlIdentifier)
   * dropMaterializedView(CqlIdentifier.fromCql(viewName)}.
   */
  @NonNull
  public static Drop dropMaterializedView(@NonNull String viewName) {
    return dropMaterializedView(CqlIdentifier.fromCql(viewName));
  }

  /** Starts a DROP MATERIALIZED VIEW query for the given view name for the given keyspace name. */
  @NonNull
  public static Drop dropMaterializedView(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier viewName) {
    return new DefaultDrop(keyspace, viewName, "MATERIALIZED VIEW");
  }

  /**
   * Shortcut for {@link #dropMaterializedView(CqlIdentifier,CqlIdentifier)
   * dropMaterializedView(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(viewName)}.
   */
  @NonNull
  public static Drop dropMaterializedView(@Nullable String keyspace, @NonNull String viewName) {
    return dropMaterializedView(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(viewName));
  }

  /**
   * Starts a CREATE TYPE query with the given type name. This assumes the keyspace name is already
   * qualified for the Session or Statement.
   */
  @NonNull
  public static CreateTypeStart createType(@NonNull CqlIdentifier typeName) {
    return new DefaultCreateType(typeName);
  }

  /** Starts a CREATE TYPE query with the given type name for the given keyspace name. */
  @NonNull
  public static CreateTypeStart createType(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier typeName) {
    return new DefaultCreateType(keyspace, typeName);
  }

  /** Shortcut for {@link #createType(CqlIdentifier) createType(CqlIdentifier.fromCql(typeName)}. */
  @NonNull
  public static CreateTypeStart createType(@NonNull String typeName) {
    return createType(CqlIdentifier.fromCql(typeName));
  }

  /**
   * Shortcut for {@link #createType(CqlIdentifier,CqlIdentifier)
   * createType(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(typeName)}.
   */
  @NonNull
  public static CreateTypeStart createType(@Nullable String keyspace, @NonNull String typeName) {
    return createType(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(typeName));
  }

  /**
   * Starts an ALTER TYPE query with the given type name. This assumes the keyspace name is already
   * qualified for the Session or Statement.
   */
  @NonNull
  public static AlterTypeStart alterType(@NonNull CqlIdentifier typeName) {
    return new DefaultAlterType(typeName);
  }

  /** Starts an ALTER TYPE query with the given type name for the given keyspace name. */
  @NonNull
  public static AlterTypeStart alterType(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier typeName) {
    return new DefaultAlterType(keyspace, typeName);
  }

  /** Shortcut for {@link #alterType(CqlIdentifier) alterType(CqlIdentifier.fromCql(typeName)} */
  @NonNull
  public static AlterTypeStart alterType(@NonNull String typeName) {
    return alterType(CqlIdentifier.fromCql(typeName));
  }

  /**
   * Shortcut for {@link #alterType(CqlIdentifier,CqlIdentifier)
   * alterType(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(typeName)}
   */
  @NonNull
  public static AlterTypeStart alterType(@Nullable String keyspace, @NonNull String typeName) {
    return alterType(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(typeName));
  }

  /**
   * Starts a DROP TYPE query. This assumes the keyspace name is already qualified for the Session
   * or Statement.
   */
  @NonNull
  public static Drop dropType(@NonNull CqlIdentifier typeName) {
    return new DefaultDrop(typeName, "TYPE");
  }

  /** Shortcut for {@link #dropType(CqlIdentifier) dropType(CqlIdentifier.fromCql(typeName)}. */
  @NonNull
  public static Drop dropType(@NonNull String typeName) {
    return dropType(CqlIdentifier.fromCql(typeName));
  }

  /** Starts a DROP TYPE query for the given view name for the given type name. */
  @NonNull
  public static Drop dropType(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier typeName) {
    return new DefaultDrop(keyspace, typeName, "TYPE");
  }

  /**
   * Shortcut for {@link #dropType(CqlIdentifier,CqlIdentifier)
   * dropType(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(typeName)}.
   */
  @NonNull
  public static Drop dropType(@Nullable String keyspace, @NonNull String typeName) {
    return dropType(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(typeName));
  }

  /**
   * Starts a CREATE INDEX query with no name. When this is used a name will be generated on the
   * server side, usually with a format of <code>tableName_idx_columnName</code>.
   */
  @NonNull
  public static CreateIndexStart createIndex() {
    return new DefaultCreateIndex();
  }

  /** Starts a CREATE INDEX query with the given name. */
  @NonNull
  public static CreateIndexStart createIndex(@Nullable CqlIdentifier indexName) {
    return new DefaultCreateIndex(indexName);
  }

  /**
   * Shortcut for {@link #createIndex(CqlIdentifier) createIndex(CqlIdentifier.fromCql(indexName)}.
   */
  @NonNull
  public static CreateIndexStart createIndex(@Nullable String indexName) {
    return createIndex(indexName == null ? null : CqlIdentifier.fromCql(indexName));
  }

  /**
   * Starts a DROP INDEX query. This assumes the keyspace name is already qualified for the Session
   * or Statement.
   */
  @NonNull
  public static Drop dropIndex(@NonNull CqlIdentifier indexName) {
    return new DefaultDrop(indexName, "INDEX");
  }

  /** Shortcut for {@link #dropIndex(CqlIdentifier) dropIndex(CqlIdentifier.fromCql(indexName)}. */
  @NonNull
  public static Drop dropIndex(@NonNull String indexName) {
    return dropIndex(CqlIdentifier.fromCql(indexName));
  }

  /** Starts a DROP INDEX query for the given index for the given keyspace name. */
  @NonNull
  public static Drop dropIndex(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier indexName) {
    return new DefaultDrop(keyspace, indexName, "INDEX");
  }

  /**
   * Shortcut for {@link #dropIndex(CqlIdentifier, CqlIdentifier)}
   * dropIndex(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(indexName)}.
   */
  @NonNull
  public static Drop dropIndex(@Nullable String keyspace, @NonNull String indexName) {
    return dropIndex(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(indexName));
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   */
  @NonNull
  public static CreateFunctionStart createFunction(@NonNull CqlIdentifier functionName) {
    return new DefaultCreateFunction(functionName);
  }

  /** Starts a CREATE FUNCTION query with the given function name for the given keyspace name. */
  @NonNull
  public static CreateFunctionStart createFunction(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier functionName) {
    return new DefaultCreateFunction(keyspace, functionName);
  }
  /**
   * Shortcut for {@link #createFunction(CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(functionName)}
   */
  @NonNull
  public static CreateFunctionStart createFunction(@NonNull String functionName) {
    return new DefaultCreateFunction(CqlIdentifier.fromCql(functionName));
  }
  /**
   * Shortcut for {@link #createFunction(CqlIdentifier, CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(keyspace, functionName)}
   */
  @NonNull
  public static CreateFunctionStart createFunction(
      @Nullable String keyspace, @NonNull String functionName) {
    return new DefaultCreateFunction(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(functionName));
  }

  /**
   * Starts a DROP FUNCTION query. This assumes the keyspace name is already qualified for the
   * Session or Statement.
   */
  @NonNull
  public static Drop dropFunction(@NonNull CqlIdentifier functionName) {
    return new DefaultDrop(functionName, "FUNCTION");
  }

  /** Starts a DROP FUNCTION query for the given function name for the given keyspace name. */
  @NonNull
  public static Drop dropFunction(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier functionName) {
    return new DefaultDrop(keyspace, functionName, "FUNCTION");
  }

  /**
   * Shortcut for {@link #dropFunction(CqlIdentifier)
   * dropFunction(CqlIdentifier.fromCql(functionName)}.
   */
  @NonNull
  public static Drop dropFunction(@NonNull String functionName) {
    return new DefaultDrop(CqlIdentifier.fromCql(functionName), "FUNCTION");
  }

  /**
   * Shortcut for {@link #dropFunction(CqlIdentifier, CqlIdentifier)
   * dropFunction(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(functionName)}.
   */
  @NonNull
  public static Drop dropFunction(@Nullable String keyspace, @NonNull String functionName) {
    return new DefaultDrop(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(functionName),
        "FUNCTION");
  }

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name. This assumes the keyspace name
   * is already qualified for the Session or Statement.
   */
  @NonNull
  public static CreateAggregateStart createAggregate(@NonNull CqlIdentifier aggregateName) {
    return new DefaultCreateAggregate(aggregateName);
  }

  /** Starts a CREATE AGGREGATE query with the given aggregate name for the given keyspace name. */
  @NonNull
  public static CreateAggregateStart createAggregate(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier aggregateName) {
    return new DefaultCreateAggregate(keyspace, aggregateName);
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier)
   * CreateAggregateStart(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(aggregateName)}
   */
  @NonNull
  public static CreateAggregateStart createAggregate(@NonNull String aggregateName) {
    return new DefaultCreateAggregate(CqlIdentifier.fromCql(aggregateName));
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier, CqlIdentifier)
   * CreateAggregateStart(CqlIdentifier.fromCql(keyspace, aggregateName)}
   */
  @NonNull
  public static CreateAggregateStart createAggregate(
      @Nullable String keyspace, @NonNull String aggregateName) {
    return new DefaultCreateAggregate(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(aggregateName));
  }

  /**
   * Starts an DROP AGGREGATE query. This assumes the keyspace name is already qualified for the
   * Session or Statement.
   */
  @NonNull
  public static Drop dropAggregate(@NonNull CqlIdentifier aggregateName) {
    return new DefaultDrop(aggregateName, "AGGREGATE");
  }

  /** Starts an DROP AGGREGATE query for the given aggregate name for the given keyspace name. */
  @NonNull
  public static Drop dropAggregate(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier aggregateName) {
    return new DefaultDrop(keyspace, aggregateName, "AGGREGATE");
  }

  /**
   * Shortcut for {@link #dropAggregate(CqlIdentifier)
   * dropAggregate(CqlIdentifier.fromCql(aggregateName)}.
   */
  @NonNull
  public static Drop dropAggregate(@NonNull String aggregateName) {
    return new DefaultDrop(CqlIdentifier.fromCql(aggregateName), "AGGREGATE");
  }

  /**
   * Shortcut for {@link #dropAggregate(CqlIdentifier, CqlIdentifier)
   * dropAggregate(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(aggregateName)}.
   */
  @NonNull
  public static Drop dropAggregate(@Nullable String keyspace, @NonNull String aggregateName) {
    return new DefaultDrop(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(aggregateName),
        "AGGREGATE");
  }

  /**
   * Compaction options for Size Tiered Compaction Strategy (STCS).
   *
   * @see CreateTableWithOptions#withCompaction(CompactionStrategy)
   */
  @NonNull
  public static SizeTieredCompactionStrategy sizeTieredCompactionStrategy() {
    return new DefaultSizeTieredCompactionStrategy();
  }

  /**
   * Compaction options for Leveled Compaction Strategy (LCS).
   *
   * @see CreateTableWithOptions#withCompaction(CompactionStrategy)
   */
  @NonNull
  public static LeveledCompactionStrategy leveledCompactionStrategy() {
    return new DefaultLeveledCompactionStrategy();
  }

  /**
   * Compaction options for Time Window Compaction Strategy (TWCS).
   *
   * @see CreateTableWithOptions#withCompaction(CompactionStrategy)
   */
  @NonNull
  public static TimeWindowCompactionStrategy timeWindowCompactionStrategy() {
    return new DefaultTimeWindowCompactionStrategy();
  }

  /**
   * Shortcut for creating a user-defined {@link DataType} for use in UDT and Table builder
   * definitions, such as {@link CreateTable#withColumn(CqlIdentifier, DataType)}.
   */
  @NonNull
  public static UserDefinedType udt(@NonNull CqlIdentifier name, boolean frozen) {
    return new ShallowUserDefinedType(null, name, frozen);
  }

  /** Shortcut for {@link #udt(CqlIdentifier,boolean) udt(CqlIdentifier.fromCql(name),frozen)}. */
  @NonNull
  public static UserDefinedType udt(@NonNull String name, boolean frozen) {
    return udt(CqlIdentifier.fromCql(name), frozen);
  }

  /**
   * Specifies the rows_per_partition configuration for table caching options.
   *
   * @see RelationOptions#withCaching(boolean, SchemaBuilder.RowsPerPartition)
   */
  public static class RowsPerPartition {

    private final String value;

    private RowsPerPartition(String value) {
      this.value = value;
    }

    @NonNull public static RowsPerPartition ALL = new RowsPerPartition("ALL");

    @NonNull public static RowsPerPartition NONE = new RowsPerPartition("NONE");

    @NonNull
    public static RowsPerPartition rows(int rowNumber) {
      return new RowsPerPartition(Integer.toString(rowNumber));
    }

    @NonNull
    public String getValue() {
      return value;
    }
  }
}
