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
package com.datastax.dse.driver.internal.querybuilder.schema;

import static com.datastax.oss.driver.internal.querybuilder.schema.Utils.appendSet;

import com.datastax.dse.driver.api.querybuilder.schema.AlterDseTableAddColumnEnd;
import com.datastax.dse.driver.api.querybuilder.schema.AlterDseTableDropColumnEnd;
import com.datastax.dse.driver.api.querybuilder.schema.AlterDseTableRenameColumnEnd;
import com.datastax.dse.driver.api.querybuilder.schema.AlterDseTableStart;
import com.datastax.dse.driver.api.querybuilder.schema.AlterDseTableWithOptionsEnd;
import com.datastax.dse.driver.api.querybuilder.schema.DseGraphEdgeSide;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.internal.querybuilder.schema.OptionsUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultAlterDseTable
    implements AlterDseTableStart,
        AlterDseTableAddColumnEnd,
        AlterDseTableDropColumnEnd,
        AlterDseTableRenameColumnEnd,
        AlterDseTableWithOptionsEnd,
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
  private final DseTableVertexOperation vertexOperation;
  private final DseTableEdgeOperation edgeOperation;
  private final ImmutableMap<String, Object> options;
  private final boolean dropCompactStorage;

  public DefaultAlterDseTable(@Nonnull CqlIdentifier tableName) {
    this(null, tableName);
  }

  public DefaultAlterDseTable(@Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier tableName) {
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
        null,
        null,
        ImmutableMap.of());
  }

  public DefaultAlterDseTable(
      @Nullable CqlIdentifier keyspace,
      @Nonnull CqlIdentifier tableName,
      boolean dropCompactStorage,
      @Nonnull ImmutableMap<CqlIdentifier, DataType> columnsToAddInOrder,
      @Nonnull ImmutableSet<CqlIdentifier> columnsToAdd,
      @Nonnull ImmutableSet<CqlIdentifier> columnsToAddStatic,
      @Nonnull ImmutableSet<CqlIdentifier> columnsToDrop,
      @Nonnull ImmutableMap<CqlIdentifier, CqlIdentifier> columnsToRename,
      @Nullable CqlIdentifier columnToAlter,
      @Nullable DataType columnToAlterType,
      @Nullable DseTableVertexOperation vertexOperation,
      @Nullable DseTableEdgeOperation edgeOperation,
      @Nonnull ImmutableMap<String, Object> options) {
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
    this.vertexOperation = vertexOperation;
    this.edgeOperation = edgeOperation;
    this.options = options;
  }

  @Nonnull
  @Override
  public AlterDseTableAddColumnEnd addColumn(
      @Nonnull CqlIdentifier columnName, @Nonnull DataType dataType) {
    return new DefaultAlterDseTable(
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
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public AlterDseTableAddColumnEnd addStaticColumn(
      @Nonnull CqlIdentifier columnName, @Nonnull DataType dataType) {
    return new DefaultAlterDseTable(
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
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public BuildableQuery dropCompactStorage() {
    return new DefaultAlterDseTable(
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
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public AlterDseTableDropColumnEnd dropColumns(@Nonnull CqlIdentifier... columnNames) {
    ImmutableSet.Builder<CqlIdentifier> builder =
        ImmutableSet.<CqlIdentifier>builder().addAll(columnsToDrop);
    for (CqlIdentifier columnName : columnNames) {
      builder = builder.add(columnName);
    }

    return new DefaultAlterDseTable(
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
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public AlterDseTableRenameColumnEnd renameColumn(
      @Nonnull CqlIdentifier from, @Nonnull CqlIdentifier to) {
    return new DefaultAlterDseTable(
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
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public BuildableQuery alterColumn(@Nonnull CqlIdentifier columnName, @Nonnull DataType dataType) {
    return new DefaultAlterDseTable(
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
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public BuildableQuery withVertexLabel(@Nullable CqlIdentifier vertexLabelId) {
    return new DefaultAlterDseTable(
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
        new DseTableVertexOperation(DseTableGraphOperationType.WITH, vertexLabelId),
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public BuildableQuery withoutVertexLabel(@Nullable CqlIdentifier vertexLabelId) {
    return new DefaultAlterDseTable(
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
        new DseTableVertexOperation(DseTableGraphOperationType.WITHOUT, vertexLabelId),
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public BuildableQuery withEdgeLabel(
      @Nullable CqlIdentifier edgeLabelId,
      @Nonnull DseGraphEdgeSide from,
      @Nonnull DseGraphEdgeSide to) {
    return new DefaultAlterDseTable(
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
        vertexOperation,
        new DseTableEdgeOperation(DseTableGraphOperationType.WITH, edgeLabelId, from, to),
        options);
  }

  @Nonnull
  @Override
  public BuildableQuery withoutEdgeLabel(@Nullable CqlIdentifier edgeLabelId) {
    return new DefaultAlterDseTable(
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
        vertexOperation,
        new DseTableEdgeOperation(DseTableGraphOperationType.WITHOUT, edgeLabelId, null, null),
        options);
  }

  @Nonnull
  @Override
  public AlterDseTableWithOptionsEnd withOption(@Nonnull String name, @Nonnull Object value) {
    return new DefaultAlterDseTable(
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
        vertexOperation,
        edgeOperation,
        ImmutableCollections.append(options, name, value));
  }

  @Nonnull
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
    } else if (vertexOperation != null) {
      builder.append(' ').append(vertexOperation.getType()).append(' ');
      vertexOperation.append(builder);
    } else if (edgeOperation != null) {
      builder.append(' ').append(edgeOperation.getType()).append(' ');
      edgeOperation.append(builder);
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

  @Nonnull
  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Nonnull
  public CqlIdentifier getTable() {
    return tableName;
  }

  @Nonnull
  public ImmutableMap<CqlIdentifier, DataType> getColumnsToAddInOrder() {
    return columnsToAddInOrder;
  }

  @Nonnull
  public ImmutableSet<CqlIdentifier> getColumnsToAddRegular() {
    return columnsToAdd;
  }

  @Nonnull
  public ImmutableSet<CqlIdentifier> getColumnsToAddStatic() {
    return columnsToAddStatic;
  }

  @Nonnull
  public ImmutableSet<CqlIdentifier> getColumnsToDrop() {
    return columnsToDrop;
  }

  @Nonnull
  public ImmutableMap<CqlIdentifier, CqlIdentifier> getColumnsToRename() {
    return columnsToRename;
  }

  @Nullable
  public CqlIdentifier getColumnToAlter() {
    return columnToAlter;
  }

  @Nullable
  public DataType getColumnToAlterType() {
    return columnToAlterType;
  }

  @Nullable
  public DseTableVertexOperation getVertexOperation() {
    return vertexOperation;
  }

  @Nullable
  public DseTableEdgeOperation getEdgeOperation() {
    return edgeOperation;
  }

  public boolean isDropCompactStorage() {
    return dropCompactStorage;
  }
}
