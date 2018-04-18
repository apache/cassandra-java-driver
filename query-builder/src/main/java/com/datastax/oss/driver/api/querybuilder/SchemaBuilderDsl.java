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
import com.datastax.oss.driver.api.querybuilder.schema.RelationStructure;
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

/** A Domain-Specific Language to build CQL DDL queries using Java code. */
public class SchemaBuilderDsl {

  /** Starts a CREATE KEYSPACE query. */
  public static CreateKeyspaceStart createKeyspace(CqlIdentifier keyspaceName) {
    return new DefaultCreateKeyspace(keyspaceName);
  }

  /**
   * Shortcut for {@link #createKeyspace(CqlIdentifier)
   * createKeyspace(CqlIdentifier.fromCql(keyspaceName))}
   */
  public static CreateKeyspaceStart createKeyspace(String keyspaceName) {
    return createKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /** Starts an ALTER KEYSPACE query. */
  public static AlterKeyspaceStart alterKeyspace(CqlIdentifier keyspaceName) {
    return new DefaultAlterKeyspace(keyspaceName);
  }

  /**
   * Shortcut for {@link #alterKeyspace(CqlIdentifier)
   * alterKeyspace(CqlIdentifier.fromCql(keyspaceName)}.
   */
  public static AlterKeyspaceStart alterKeyspace(String keyspaceName) {
    return alterKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /** Starts a DROP KEYSPACE query. */
  public static Drop dropKeyspace(CqlIdentifier keyspaceName) {
    return new DefaultDropKeyspace(keyspaceName);
  }

  /**
   * Shortcut for {@link #dropKeyspace(CqlIdentifier)
   * dropKeyspace(CqlIdentifier.fromCql(keyspaceName)}.
   */
  public static Drop dropKeyspace(String keyspaceName) {
    return dropKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /**
   * Starts a CREATE TABLE query with the given table name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   */
  public static CreateTableStart createTable(CqlIdentifier tableName) {
    return new DefaultCreateTable(tableName);
  }

  /** Starts a CREATE TABLE query with the given table name for the given keyspace name. */
  public static CreateTableStart createTable(CqlIdentifier keyspace, CqlIdentifier tableName) {
    return new DefaultCreateTable(keyspace, tableName);
  }

  /**
   * Shortcut for {@link #createTable(CqlIdentifier) createTable(CqlIdentifier.fromCql(tableName)}
   */
  public static CreateTableStart createTable(String tableName) {
    return createTable(CqlIdentifier.fromCql(tableName));
  }

  /**
   * Shortcut for {@link #createTable(CqlIdentifier,CqlIdentifier)
   * createTable(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(tableName)}
   */
  public static CreateTableStart createTable(String keyspace, String tableName) {
    return createTable(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(tableName));
  }

  /**
   * Starts an ALTER TABLE query with the given table name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   */
  public static AlterTableStart alterTable(CqlIdentifier tableName) {
    return new DefaultAlterTable(tableName);
  }

  /** Starts an ALTER TABLE query with the given table name for the given keyspace name. */
  public static AlterTableStart alterTable(CqlIdentifier keyspace, CqlIdentifier tableName) {
    return new DefaultAlterTable(keyspace, tableName);
  }

  /** Shortcut for {@link #alterTable(CqlIdentifier) alterTable(CqlIdentifier.fromCql(tableName)} */
  public static AlterTableStart alterTable(String tableName) {
    return alterTable(CqlIdentifier.fromCql(tableName));
  }

  /**
   * Shortcut for {@link #alterTable(CqlIdentifier,CqlIdentifier)
   * alterTable(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(tableName)}
   */
  public static AlterTableStart alterTable(String keyspace, String tableName) {
    return alterTable(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(tableName));
  }

  /**
   * Starts a DROP TABLE query. This assumes the keyspace name is already qualified for the Session
   * or Statement.
   */
  public static Drop dropTable(CqlIdentifier tableName) {
    return new DefaultDrop(tableName, "TABLE");
  }

  /** Shortcut for {@link #dropTable(CqlIdentifier) dropTable(CqlIdentifier.fromCql(tableName)}. */
  public static Drop dropTable(String tableName) {
    return dropTable(CqlIdentifier.fromCql(tableName));
  }

  /** Starts a DROP TABLE query for the given table name for the given keyspace name. */
  public static Drop dropTable(CqlIdentifier keyspace, CqlIdentifier tableName) {
    return new DefaultDrop(keyspace, tableName, "TABLE");
  }

  /**
   * Shortcut for {@link #dropTable(CqlIdentifier,CqlIdentifier)
   * dropTable(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(tableName)}.
   */
  public static Drop dropTable(String keyspace, String tableName) {
    return dropTable(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(tableName));
  }

  /**
   * Starts a CREATE MATERIALIZED VIEW query with the given view name. This assumes the keyspace
   * name is already qualified for the Session or Statement.
   */
  public static CreateMaterializedViewStart createMaterializedView(CqlIdentifier viewName) {
    return new DefaultCreateMaterializedView(viewName);
  }

  /**
   * Starts a CREATE MATERIALIZED VIEW query with the given view name for the given keyspace name.
   */
  public static CreateMaterializedViewStart createMaterializedView(
      CqlIdentifier keyspace, CqlIdentifier viewName) {
    return new DefaultCreateMaterializedView(keyspace, viewName);
  }

  /**
   * Shortcut for {@link #createMaterializedView(CqlIdentifier)
   * createMaterializedView(CqlIdentifier.fromCql(viewName)}
   */
  public static CreateMaterializedViewStart createMaterializedView(String viewName) {
    return createMaterializedView(CqlIdentifier.fromCql(viewName));
  }

  /**
   * Shortcut for {@link #createMaterializedView(CqlIdentifier,CqlIdentifier)
   * createMaterializedView(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(viewName)}
   */
  public static CreateMaterializedViewStart createMaterializedView(
      String keyspace, String viewName) {
    return createMaterializedView(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(viewName));
  }

  /**
   * Starts an ALTER MATERIALIZED VIEW query with the given view name. This assumes the keyspace
   * name is already qualified for the Session or Statement.
   */
  public static AlterMaterializedViewStart alterMaterializedView(CqlIdentifier viewName) {
    return new DefaultAlterMaterializedView(viewName);
  }

  /**
   * Starts an ALTER MATERIALIZED VIEW query with the given view name for the given keyspace name.
   */
  public static AlterMaterializedViewStart alterMaterializedView(
      CqlIdentifier keyspace, CqlIdentifier viewName) {
    return new DefaultAlterMaterializedView(keyspace, viewName);
  }

  /**
   * Shortcut for {@link #alterMaterializedView(CqlIdentifier)
   * alterMaterializedView(CqlIdentifier.fromCql(viewName)}
   */
  public static AlterMaterializedViewStart alterMaterializedView(String viewName) {
    return alterMaterializedView(CqlIdentifier.fromCql(viewName));
  }

  /**
   * Shortcut for {@link #alterMaterializedView(CqlIdentifier,CqlIdentifier)
   * alterMaterializedView(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(viewName)}
   */
  public static AlterMaterializedViewStart alterMaterializedView(String keyspace, String viewName) {
    return alterMaterializedView(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(viewName));
  }

  /**
   * Starts a DROP MATERIALIZED VIEW query. This assumes the keyspace name is already qualified for
   * the Session or Statement.
   */
  public static Drop dropMaterializedView(CqlIdentifier viewName) {
    return new DefaultDrop(viewName, "MATERIALIZED VIEW");
  }

  /**
   * Shortcut for {@link #dropMaterializedView(CqlIdentifier)
   * dropMaterializedView(CqlIdentifier.fromCql(viewName)}.
   */
  public static Drop dropMaterializedView(String viewName) {
    return dropMaterializedView(CqlIdentifier.fromCql(viewName));
  }

  /** Starts a DROP MATERIALIZED VIEW query for the given view name for the given keyspace name. */
  public static Drop dropMaterializedView(CqlIdentifier keyspace, CqlIdentifier viewName) {
    return new DefaultDrop(keyspace, viewName, "MATERIALIZED VIEW");
  }

  /**
   * Shortcut for {@link #dropMaterializedView(CqlIdentifier,CqlIdentifier)
   * dropMaterializedView(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(viewName)}.
   */
  public static Drop dropMaterializedView(String keyspace, String viewName) {
    return dropMaterializedView(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(viewName));
  }

  /**
   * Starts a CREATE TYPE query with the given type name. This assumes the keyspace name is already
   * qualified for the Session or Statement.
   */
  public static CreateTypeStart createType(CqlIdentifier typeName) {
    return new DefaultCreateType(typeName);
  }

  /** Starts a CREATE TYPE query with the given type name for the given keyspace name. */
  public static CreateTypeStart createType(CqlIdentifier keyspace, CqlIdentifier typeName) {
    return new DefaultCreateType(keyspace, typeName);
  }

  /** Shortcut for {@link #createType(CqlIdentifier) createType(CqlIdentifier.fromCql(typeName)}. */
  public static CreateTypeStart createType(String typeName) {
    return createType(CqlIdentifier.fromCql(typeName));
  }

  /**
   * Shortcut for {@link #createType(CqlIdentifier,CqlIdentifier)
   * createType(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(typeName)}.
   */
  public static CreateTypeStart createType(String keyspace, String typeName) {
    return createType(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(typeName));
  }

  /**
   * Starts an ALTER TYPE query with the given type name. This assumes the keyspace name is already
   * qualified for the Session or Statement.
   */
  public static AlterTypeStart alterType(CqlIdentifier typeName) {
    return new DefaultAlterType(typeName);
  }

  /** Starts an ALTER TYPE query with the given type name for the given keyspace name. */
  public static AlterTypeStart alterType(CqlIdentifier keyspace, CqlIdentifier typeName) {
    return new DefaultAlterType(keyspace, typeName);
  }

  /** Shortcut for {@link #alterType(CqlIdentifier) alterType(CqlIdentifier.fromCql(typeName)} */
  public static AlterTypeStart alterType(String typeName) {
    return alterType(CqlIdentifier.fromCql(typeName));
  }

  /**
   * Shortcut for {@link #alterType(CqlIdentifier,CqlIdentifier)
   * alterType(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(typeName)}
   */
  public static AlterTypeStart alterType(String keyspace, String typeName) {
    return alterType(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(typeName));
  }

  /**
   * Starts a DROP TYPE query. This assumes the keyspace name is already qualified for the Session
   * or Statement.
   */
  public static Drop dropType(CqlIdentifier typeName) {
    return new DefaultDrop(typeName, "TYPE");
  }

  /** Shortcut for {@link #dropType(CqlIdentifier) dropType(CqlIdentifier.fromCql(typeName)}. */
  public static Drop dropType(String typeName) {
    return dropType(CqlIdentifier.fromCql(typeName));
  }

  /** Starts a DROP TYPE query for the given view name for the given type name. */
  public static Drop dropType(CqlIdentifier keyspace, CqlIdentifier typeName) {
    return new DefaultDrop(keyspace, typeName, "TYPE");
  }

  /**
   * Shortcut for {@link #dropType(CqlIdentifier,CqlIdentifier)
   * dropType(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(typeName)}.
   */
  public static Drop dropType(String keyspace, String typeName) {
    return dropType(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(typeName));
  }

  /**
   * Starts a CREATE INDEX query with no name. When this is used a name will be generated on the
   * server side, usually with a format of <code>tableName_idx_columnName</code>.
   */
  public static CreateIndexStart createIndex() {
    return new DefaultCreateIndex();
  }

  /** Starts a CREATE INDEX query with the given name. */
  public static CreateIndexStart createIndex(CqlIdentifier indexName) {
    return new DefaultCreateIndex(indexName);
  }

  /**
   * Shortcut for {@link #createIndex(CqlIdentifier) createIndex(CqlIdentifier.fromCql(indexName)}.
   */
  public static CreateIndexStart createIndex(String indexName) {
    return createIndex(CqlIdentifier.fromCql(indexName));
  }

  /**
   * Starts a DROP INDEX query. This assumes the keyspace name is already qualified for the Session
   * or Statement.
   */
  public static Drop dropIndex(CqlIdentifier indexName) {
    return new DefaultDrop(indexName, "INDEX");
  }

  /** Shortcut for {@link #dropIndex(CqlIdentifier) dropIndex(CqlIdentifier.fromCql(indexName)}. */
  public static Drop dropIndex(String indexName) {
    return dropIndex(CqlIdentifier.fromCql(indexName));
  }

  /** Starts a DROP INDEX query for the given index for the given keyspace name. */
  public static Drop dropIndex(CqlIdentifier keyspace, CqlIdentifier indexName) {
    return new DefaultDrop(keyspace, indexName, "INDEX");
  }

  /**
   * Shortcut for {@link #dropIndex(CqlIdentifier, CqlIdentifier)}
   * dropIndex(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(indexName)}.
   */
  public static Drop dropIndex(String keyspace, String indexName) {
    return dropIndex(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(indexName));
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   */
  public static CreateFunctionStart createFunction(CqlIdentifier functionName) {
    return new DefaultCreateFunction(functionName);
  }

  /** Starts a CREATE FUNCTION query with the given function name for the given keyspace name. */
  public static CreateFunctionStart createFunction(
      CqlIdentifier keyspace, CqlIdentifier functionName) {
    return new DefaultCreateFunction(keyspace, functionName);
  }
  /**
   * Shortcut for {@link #createFunction(CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(functionName)}
   */
  public static CreateFunctionStart createFunction(String functionName) {
    return new DefaultCreateFunction(CqlIdentifier.fromCql(functionName));
  }
  /**
   * Shortcut for {@link #createFunction(CqlIdentifier, CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(keyspace, functionName)}
   */
  public static CreateFunctionStart createFunction(String keyspace, String functionName) {
    return new DefaultCreateFunction(
        CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(functionName));
  }

  /**
   * Starts a DROP FUNCTION query. This assumes the keyspace name is already qualified for the
   * Session or Statement.
   */
  public static Drop dropFunction(CqlIdentifier functionName) {
    return new DefaultDrop(functionName, "FUNCTION");
  }

  /** Starts a DROP FUNCTION query for the given function name for the given keyspace name. */
  public static Drop dropFunction(CqlIdentifier keyspace, CqlIdentifier functionName) {
    return new DefaultDrop(keyspace, functionName, "FUNCTION");
  }

  /**
   * Shortcut for {@link #dropFunction(CqlIdentifier)
   * dropFunction(CqlIdentifier.fromCql(functionName)}.
   */
  public static Drop dropFunction(String functionName) {
    return new DefaultDrop(CqlIdentifier.fromCql(functionName), "FUNCTION");
  }

  /**
   * Shortcut for {@link #dropFunction(CqlIdentifier, CqlIdentifier)
   * dropFunction(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(functionName)}.
   */
  public static Drop dropFunction(String keyspace, String functionName) {
    return new DefaultDrop(
        CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(functionName), "FUNCTION");
  }

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name. This assumes the keyspace name
   * is already qualified for the Session or Statement.
   */
  public static CreateAggregateStart createAggregate(CqlIdentifier aggregateName) {
    return new DefaultCreateAggregate(aggregateName);
  }

  /** Starts a CREATE AGGREGATE query with the given aggregate name for the given keyspace name. */
  public static CreateAggregateStart createAggregate(
      CqlIdentifier keyspace, CqlIdentifier aggregateName) {
    return new DefaultCreateAggregate(keyspace, aggregateName);
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier)
   * CreateAggregateStart(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(aggregateName)}
   */
  public static CreateAggregateStart createAggregate(String aggregateName) {
    return new DefaultCreateAggregate(CqlIdentifier.fromCql(aggregateName));
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier, CqlIdentifier)
   * CreateAggregateStart(CqlIdentifier.fromCql(keyspace, aggregateName)}
   */
  public static CreateAggregateStart createAggregate(String keyspace, String aggregateName) {
    return new DefaultCreateAggregate(
        CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(aggregateName));
  }

  /**
   * Starts an DROP AGGREGATE query. This assumes the keyspace name is already qualified for the
   * Session or Statement.
   */
  public static Drop dropAggregate(CqlIdentifier aggregateName) {
    return new DefaultDrop(aggregateName, "AGGREGATE");
  }

  /** Starts an DROP AGGREGATE query for the given aggregate name for the given keyspace name. */
  public static Drop dropAggregate(CqlIdentifier keyspace, CqlIdentifier aggregateName) {
    return new DefaultDrop(keyspace, aggregateName, "AGGREGATE");
  }

  /**
   * Shortcut for {@link #dropAggregate(CqlIdentifier)
   * dropAggregate(CqlIdentifier.fromCql(aggregateName)}.
   */
  public static Drop dropAggregate(String aggregateName) {
    return new DefaultDrop(CqlIdentifier.fromCql(aggregateName), "AGGREGATE");
  }

  /**
   * Shortcut for {@link #dropAggregate(CqlIdentifier, CqlIdentifier)
   * dropAggregate(CqlIdentifier.fromCql(keyspace),CqlIdentifier.fromCql(aggregateName)}.
   */
  public static Drop dropAggregate(String keyspace, String aggregateName) {
    return new DefaultDrop(
        CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(aggregateName), "AGGREGATE");
  }

  /**
   * Compaction options for Size Tiered Compaction Strategy (STCS).
   *
   * @see CreateTableWithOptions#withCompaction(CompactionStrategy)
   */
  public static SizeTieredCompactionStrategy sizeTieredCompactionStrategy() {
    return new DefaultSizeTieredCompactionStrategy();
  }

  /**
   * Compaction options for Leveled Compaction Strategy (LCS).
   *
   * @see CreateTableWithOptions#withCompaction(CompactionStrategy)
   */
  public static LeveledCompactionStrategy leveledCompactionStrategy() {
    return new DefaultLeveledCompactionStrategy();
  }

  /**
   * Compaction options for Time Window Compaction Strategy (TWCS).
   *
   * @see CreateTableWithOptions#withCompaction(CompactionStrategy)
   */
  public static TimeWindowCompactionStrategy timeWindowCompactionStrategy() {
    return new DefaultTimeWindowCompactionStrategy();
  }

  /**
   * Shortcut for creating a user-defined {@link DataType} for use in UDT and Table builder
   * definitions, such as {@link CreateTable#withColumn(CqlIdentifier, DataType)}.
   */
  public static UserDefinedType udt(CqlIdentifier name, boolean frozen) {
    return new ShallowUserDefinedType(null, name, frozen);
  }

  /** Shortcut for {@link #udt(CqlIdentifier,boolean) udt(CqlIdentifier.fromCql(name),frozen)}. */
  public static UserDefinedType udt(String name, boolean frozen) {
    return udt(CqlIdentifier.fromCql(name), frozen);
  }

  /**
   * Specifies the rows_per_partition configuration for table caching options.
   *
   * @see RelationStructure#withCaching(boolean, RowsPerPartition)
   */
  public static class RowsPerPartition {

    private final String value;

    private RowsPerPartition(String value) {
      this.value = value;
    }

    public static RowsPerPartition ALL = new RowsPerPartition("ALL");

    public static RowsPerPartition NONE = new RowsPerPartition("NONE");

    public static RowsPerPartition rows(int rowNumber) {
      return new RowsPerPartition(Integer.toString(rowNumber));
    }

    public String getValue() {
      return value;
    }
  }
}
