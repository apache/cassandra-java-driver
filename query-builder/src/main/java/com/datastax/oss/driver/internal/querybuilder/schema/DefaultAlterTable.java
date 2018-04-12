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
package com.datastax.oss.driver.internal.querybuilder.schema;

import static com.datastax.oss.driver.internal.querybuilder.schema.Utils.appendSet;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableDropColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableRenameColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableWithOptionsEnd;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultAlterTable
    implements AlterTableStart,
        AlterTableAddColumnEnd,
        AlterTableDropColumnEnd,
        AlterTableRenameColumnEnd,
        AlterTableWithOptionsEnd,
        BuildableQuery {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier tableName;

  private final ImmutableMap<CqlIdentifier, DataType> columnsToAddInOrder;
  private final ImmutableSet<CqlIdentifier> columnsToAdd;
  private final ImmutableSet<CqlIdentifier> columnsToAddStatic;
  private final ImmutableSet<CqlIdentifier> columnsToDrop;
  private final ImmutableMap<CqlIdentifier, CqlIdentifier> columnsToRename;
  private final CqlIdentifier columnToAlter;
  private final DataType columnToAlterType;
  private final ImmutableMap<String, Object> options;
  private final boolean dropCompactStorage;

  public DefaultAlterTable(CqlIdentifier tableName) {
    this(null, tableName);
  }

  public DefaultAlterTable(CqlIdentifier keyspace, CqlIdentifier tableName) {
    this(
        keyspace,
        tableName,
        false,
        ImmutableMap.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableMap.of(),
        null,
        null,
        ImmutableMap.of());
  }

  public DefaultAlterTable(
      CqlIdentifier keyspace,
      CqlIdentifier tableName,
      boolean dropCompactStorage,
      ImmutableMap<CqlIdentifier, DataType> columnsToAddInOrder,
      ImmutableSet<CqlIdentifier> columnsToAdd,
      ImmutableSet<CqlIdentifier> columnsToAddStatic,
      ImmutableSet<CqlIdentifier> columnsToDrop,
      ImmutableMap<CqlIdentifier, CqlIdentifier> columnsToRename,
      CqlIdentifier columnToAlter,
      DataType columnToAlterType,
      ImmutableMap<String, Object> options) {
    this.keyspace = keyspace;
    this.tableName = tableName;
    this.dropCompactStorage = dropCompactStorage;
    this.columnsToAddInOrder = columnsToAddInOrder;
    this.columnsToAdd = columnsToAdd;
    this.columnsToAddStatic = columnsToAddStatic;
    this.columnsToDrop = columnsToDrop;
    this.columnsToRename = columnsToRename;
    this.columnToAlter = columnToAlter;
    this.columnToAlterType = columnToAlterType;
    this.options = options;
  }

  @Override
  public AlterTableAddColumnEnd addColumn(CqlIdentifier columnName, DataType dataType) {
    return new DefaultAlterTable(
        keyspace,
        tableName,
        dropCompactStorage,
        ImmutableCollections.append(columnsToAddInOrder, columnName, dataType),
        appendSet(columnsToAdd, columnName),
        columnsToAddStatic,
        columnsToDrop,
        columnsToRename,
        columnToAlter,
        columnToAlterType,
        options);
  }

  @Override
  public AlterTableAddColumnEnd addStaticColumn(CqlIdentifier columnName, DataType dataType) {
    return new DefaultAlterTable(
        keyspace,
        tableName,
        dropCompactStorage,
        ImmutableCollections.append(columnsToAddInOrder, columnName, dataType),
        columnsToAdd,
        appendSet(columnsToAddStatic, columnName),
        columnsToDrop,
        columnsToRename,
        columnToAlter,
        columnToAlterType,
        options);
  }

  @Override
  public BuildableQuery dropCompactStorage() {
    return new DefaultAlterTable(
        keyspace,
        tableName,
        true,
        columnsToAddInOrder,
        columnsToAdd,
        columnsToAddStatic,
        columnsToDrop,
        columnsToRename,
        columnToAlter,
        columnToAlterType,
        options);
  }

  @Override
  public AlterTableDropColumnEnd dropColumns(CqlIdentifier... columnNames) {
    ImmutableSet.Builder<CqlIdentifier> builder =
        ImmutableSet.<CqlIdentifier>builder().addAll(columnsToDrop);
    for (CqlIdentifier columnName : columnNames) {
      builder = builder.add(columnName);
    }

    return new DefaultAlterTable(
        keyspace,
        tableName,
        dropCompactStorage,
        columnsToAddInOrder,
        columnsToAdd,
        columnsToAddStatic,
        builder.build(),
        columnsToRename,
        columnToAlter,
        columnToAlterType,
        options);
  }

  @Override
  public AlterTableRenameColumnEnd renameColumn(CqlIdentifier from, CqlIdentifier to) {
    return new DefaultAlterTable(
        keyspace,
        tableName,
        dropCompactStorage,
        columnsToAddInOrder,
        columnsToAdd,
        columnsToAddStatic,
        columnsToDrop,
        ImmutableCollections.append(columnsToRename, from, to),
        columnToAlter,
        columnToAlterType,
        options);
  }

  @Override
  public BuildableQuery alterColumn(CqlIdentifier columnName, DataType dataType) {
    return new DefaultAlterTable(
        keyspace,
        tableName,
        dropCompactStorage,
        columnsToAddInOrder,
        columnsToAdd,
        columnsToAddStatic,
        columnsToDrop,
        columnsToRename,
        columnName,
        dataType,
        options);
  }

  @Override
  public AlterTableWithOptionsEnd withOption(String name, Object value) {
    return new DefaultAlterTable(
        keyspace,
        tableName,
        dropCompactStorage,
        columnsToAddInOrder,
        columnsToAdd,
        columnsToAddStatic,
        columnsToDrop,
        columnsToRename,
        columnToAlter,
        columnToAlterType,
        ImmutableCollections.append(options, name, value));
  }

  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("ALTER TABLE ");

    CqlHelper.qualify(keyspace, tableName, builder);

    if (columnToAlter != null) {
      return builder
          .append(" ALTER ")
          .append(columnToAlter.asCql(true))
          .append(" TYPE ")
          .append(columnToAlterType.asCql(true, true))
          .toString();
    } else if (!columnsToAdd.isEmpty()) {
      builder.append(" ADD ");
      if (columnsToAdd.size() > 1) {
        builder.append('(');
      }
      boolean first = true;
      for (Map.Entry<CqlIdentifier, DataType> column : columnsToAddInOrder.entrySet()) {
        if (first) {
          first = false;
        } else {
          builder.append(',');
        }
        builder
            .append(column.getKey().asCql(true))
            .append(' ')
            .append(column.getValue().asCql(true, true));

        if (columnsToAddStatic.contains(column.getKey())) {
          builder.append(" STATIC");
        }
      }
      if (columnsToAdd.size() > 1) {
        builder.append(')');
      }
      return builder.toString();
    } else if (!columnsToDrop.isEmpty()) {
      boolean moreThanOneDrop = columnsToDrop.size() > 1;
      CqlHelper.appendIds(
          columnsToDrop,
          builder,
          moreThanOneDrop ? " DROP (" : " DROP ",
          ",",
          moreThanOneDrop ? ")" : "");
      return builder.toString();
    } else if (!columnsToRename.isEmpty()) {
      builder.append(" RENAME ");
      boolean first = true;
      for (Map.Entry<CqlIdentifier, CqlIdentifier> entry : columnsToRename.entrySet()) {
        if (first) {
          first = false;
        } else {
          builder.append(" AND ");
        }
        builder
            .append(entry.getKey().asCql(true))
            .append(" TO ")
            .append(entry.getValue().asCql(true));
      }
      return builder.toString();
    } else if (dropCompactStorage) {
      return builder.append(" DROP COMPACT STORAGE").toString();
    } else if (!options.isEmpty()) {
      return builder.append(OptionsUtils.buildOptions(options, true)).toString();
    }

    // While this is incomplete, we should return partially build query at this point for toString
    // purposes.
    return builder.toString();
  }

  @Override
  public String toString() {
    return asCql();
  }

  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  public CqlIdentifier getTable() {
    return tableName;
  }

  public ImmutableMap<CqlIdentifier, DataType> getColumnsToAddInOrder() {
    return columnsToAddInOrder;
  }

  public ImmutableSet<CqlIdentifier> getColumnsToAddRegular() {
    return columnsToAdd;
  }

  public ImmutableSet<CqlIdentifier> getColumnsToAddStatic() {
    return columnsToAddStatic;
  }

  public ImmutableSet<CqlIdentifier> getColumnsToDrop() {
    return columnsToDrop;
  }

  public ImmutableMap<CqlIdentifier, CqlIdentifier> getColumnsToRename() {
    return columnsToRename;
  }

  public CqlIdentifier getColumnToAlter() {
    return columnToAlter;
  }

  public DataType getColumnToAlterType() {
    return columnToAlterType;
  }

  public boolean isDropCompactStorage() {
    return dropCompactStorage;
  }
}
