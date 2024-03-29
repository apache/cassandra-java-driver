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
package com.datastax.dse.driver.internal.core.metadata.schema.parsing;

import com.datastax.dse.driver.api.core.metadata.schema.DseColumnMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseEdgeMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseIndexMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseTableMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseVertexMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseColumnMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseEdgeMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseIndexMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseTableMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseVertexMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.CqlIdentifiers;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeClassNameCompositeParser;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.RawColumn;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.RelationParser;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.TableParser;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DseTableParser extends RelationParser {

  private static final Logger LOG = LoggerFactory.getLogger(TableParser.class);

  public DseTableParser(SchemaRows rows, InternalDriverContext context) {
    super(rows, context);
  }

  public DseTableMetadata parseTable(
      AdminRow tableRow,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userTypes,
      Multimap<CqlIdentifier, AdminRow> vertices,
      Multimap<CqlIdentifier, AdminRow> edges) {
    // Cassandra <= 2.2:
    // CREATE TABLE system.schema_columnfamilies (
    //     keyspace_name text,
    //     columnfamily_name text,
    //     bloom_filter_fp_chance double,
    //     caching text,
    //     cf_id uuid,
    //     column_aliases text, (2.1 only)
    //     comment text,
    //     compaction_strategy_class text,
    //     compaction_strategy_options text,
    //     comparator text,
    //     compression_parameters text,
    //     default_time_to_live int,
    //     default_validator text,
    //     dropped_columns map<text, bigint>,
    //     gc_grace_seconds int,
    //     index_interval int,
    //     is_dense boolean, (2.1 only)
    //     key_aliases text, (2.1 only)
    //     key_validator text,
    //     local_read_repair_chance double,
    //     max_compaction_threshold int,
    //     max_index_interval int,
    //     memtable_flush_period_in_ms int,
    //     min_compaction_threshold int,
    //     min_index_interval int,
    //     read_repair_chance double,
    //     speculative_retry text,
    //     subcomparator text,
    //     type text,
    //     value_alias text, (2.1 only)
    //     PRIMARY KEY (keyspace_name, columnfamily_name)
    // ) WITH CLUSTERING ORDER BY (columnfamily_name ASC)
    //
    // Cassandra 3.0:
    // CREATE TABLE system_schema.tables (
    //     keyspace_name text,
    //     table_name text,
    //     bloom_filter_fp_chance double,
    //     caching frozen<map<text, text>>,
    //     cdc boolean,
    //     comment text,
    //     compaction frozen<map<text, text>>,
    //     compression frozen<map<text, text>>,
    //     crc_check_chance double,
    //     dclocal_read_repair_chance double,
    //     default_time_to_live int,
    //     extensions frozen<map<text, blob>>,
    //     flags frozen<set<text>>,
    //     gc_grace_seconds int,
    //     id uuid,
    //     max_index_interval int,
    //     memtable_flush_period_in_ms int,
    //     min_index_interval int,
    //     read_repair_chance double,
    //     speculative_retry text,
    //     PRIMARY KEY (keyspace_name, table_name)
    // ) WITH CLUSTERING ORDER BY (table_name ASC)
    CqlIdentifier tableId =
        CqlIdentifier.fromInternal(
            tableRow.getString(
                tableRow.contains("table_name") ? "table_name" : "columnfamily_name"));

    UUID uuid = tableRow.contains("id") ? tableRow.getUuid("id") : tableRow.getUuid("cf_id");

    List<RawColumn> rawColumns =
        RawColumn.toRawColumns(
            rows.columns().getOrDefault(keyspaceId, ImmutableMultimap.of()).get(tableId));
    if (rawColumns.isEmpty()) {
      LOG.warn(
          "[{}] Processing TABLE refresh for {}.{} but found no matching rows, skipping",
          logPrefix,
          keyspaceId,
          tableId);
      return null;
    }

    boolean isCompactStorage;
    if (tableRow.contains("flags")) {
      Set<String> flags = tableRow.getSetOfString("flags");
      boolean isDense = flags.contains("dense");
      boolean isSuper = flags.contains("super");
      boolean isCompound = flags.contains("compound");
      isCompactStorage = isSuper || isDense || !isCompound;
      boolean isStaticCompact = !isSuper && !isDense && !isCompound;
      if (isStaticCompact) {
        RawColumn.pruneStaticCompactTableColumns(rawColumns);
      } else if (isDense) {
        RawColumn.pruneDenseTableColumnsV3(rawColumns);
      }
    } else {
      boolean isDense = tableRow.getBoolean("is_dense");
      if (isDense) {
        RawColumn.pruneDenseTableColumnsV2(rawColumns);
      }
      DataTypeClassNameCompositeParser.ParseResult comparator =
          new DataTypeClassNameCompositeParser()
              .parseWithComposite(tableRow.getString("comparator"), keyspaceId, userTypes, context);
      isCompactStorage = isDense || !comparator.isComposite;
    }

    Collections.sort(rawColumns);
    ImmutableMap.Builder<CqlIdentifier, ColumnMetadata> allColumnsBuilder = ImmutableMap.builder();
    ImmutableList.Builder<ColumnMetadata> partitionKeyBuilder = ImmutableList.builder();
    ImmutableMap.Builder<ColumnMetadata, ClusteringOrder> clusteringColumnsBuilder =
        ImmutableMap.builder();
    ImmutableMap.Builder<CqlIdentifier, IndexMetadata> indexesBuilder = ImmutableMap.builder();

    for (RawColumn raw : rawColumns) {
      DataType dataType = rows.dataTypeParser().parse(keyspaceId, raw.dataType, userTypes, context);
      DseColumnMetadata column =
          new DefaultDseColumnMetadata(
              keyspaceId, tableId, raw.name, dataType, raw.kind.equals(RawColumn.KIND_STATIC));
      switch (raw.kind) {
        case RawColumn.KIND_PARTITION_KEY:
          partitionKeyBuilder.add(column);
          break;
        case RawColumn.KIND_CLUSTERING_COLUMN:
          clusteringColumnsBuilder.put(
              column, raw.reversed ? ClusteringOrder.DESC : ClusteringOrder.ASC);
          break;
        default:
          // nothing to do
      }
      allColumnsBuilder.put(column.getName(), column);

      DseIndexMetadata index = buildLegacyIndex(raw, column);
      if (index != null) {
        indexesBuilder.put(index.getName(), index);
      }
    }

    Map<CqlIdentifier, Object> options;
    try {
      options = parseOptions(tableRow);
    } catch (Exception e) {
      // Options change the most often, so be especially lenient if anything goes wrong.
      Loggers.warnWithException(
          LOG,
          "[{}] Error while parsing options for {}.{}, getOptions() will be empty",
          logPrefix,
          keyspaceId,
          tableId,
          e);
      options = Collections.emptyMap();
    }

    Collection<AdminRow> indexRows =
        rows.indexes().getOrDefault(keyspaceId, ImmutableMultimap.of()).get(tableId);
    for (AdminRow indexRow : indexRows) {
      DseIndexMetadata index = buildModernIndex(keyspaceId, tableId, indexRow);
      indexesBuilder.put(index.getName(), index);
    }

    return new DefaultDseTableMetadata(
        keyspaceId,
        tableId,
        uuid,
        isCompactStorage,
        false,
        partitionKeyBuilder.build(),
        clusteringColumnsBuilder.build(),
        allColumnsBuilder.build(),
        options,
        indexesBuilder.build(),
        buildVertex(tableId, vertices),
        buildEdge(tableId, edges, vertices));
  }

  DseTableMetadata parseVirtualTable(AdminRow tableRow, CqlIdentifier keyspaceId) {

    CqlIdentifier tableId = CqlIdentifier.fromInternal(tableRow.getString("table_name"));

    List<RawColumn> rawColumns =
        RawColumn.toRawColumns(
            rows.virtualColumns().getOrDefault(keyspaceId, ImmutableMultimap.of()).get(tableId));
    if (rawColumns.isEmpty()) {
      LOG.warn(
          "[{}] Processing TABLE refresh for {}.{} but found no matching rows, skipping",
          logPrefix,
          keyspaceId,
          tableId);
      return null;
    }

    Collections.sort(rawColumns);
    ImmutableMap.Builder<CqlIdentifier, ColumnMetadata> allColumnsBuilder = ImmutableMap.builder();
    ImmutableList.Builder<ColumnMetadata> partitionKeyBuilder = ImmutableList.builder();
    ImmutableMap.Builder<ColumnMetadata, ClusteringOrder> clusteringColumnsBuilder =
        ImmutableMap.builder();

    for (RawColumn raw : rawColumns) {
      DataType dataType =
          rows.dataTypeParser().parse(keyspaceId, raw.dataType, Collections.emptyMap(), context);
      DseColumnMetadata column =
          new DefaultDseColumnMetadata(
              keyspaceId, tableId, raw.name, dataType, raw.kind.equals(RawColumn.KIND_STATIC));
      switch (raw.kind) {
        case RawColumn.KIND_PARTITION_KEY:
          partitionKeyBuilder.add(column);
          break;
        case RawColumn.KIND_CLUSTERING_COLUMN:
          clusteringColumnsBuilder.put(
              column, raw.reversed ? ClusteringOrder.DESC : ClusteringOrder.ASC);
          break;
        default:
      }

      allColumnsBuilder.put(column.getName(), column);
    }

    return new DefaultDseTableMetadata(
        keyspaceId,
        tableId,
        null,
        false,
        true,
        partitionKeyBuilder.build(),
        clusteringColumnsBuilder.build(),
        allColumnsBuilder.build(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        null,
        null);
  }

  // In C*<=2.2, index information is stored alongside the column.
  private DseIndexMetadata buildLegacyIndex(RawColumn raw, ColumnMetadata column) {
    if (raw.indexName == null) {
      return null;
    }
    return new DefaultDseIndexMetadata(
        column.getKeyspace(),
        column.getParent(),
        CqlIdentifier.fromInternal(raw.indexName),
        IndexKind.valueOf(raw.indexType),
        buildLegacyIndexTarget(column, raw.indexOptions),
        raw.indexOptions);
  }

  private static String buildLegacyIndexTarget(ColumnMetadata column, Map<String, String> options) {
    String columnName = column.getName().asCql(true);
    DataType columnType = column.getType();
    if (options.containsKey("index_keys")) {
      return String.format("keys(%s)", columnName);
    }
    if (options.containsKey("index_keys_and_values")) {
      return String.format("entries(%s)", columnName);
    }
    if ((columnType instanceof ListType && ((ListType) columnType).isFrozen())
        || (columnType instanceof SetType && ((SetType) columnType).isFrozen())
        || (columnType instanceof MapType && ((MapType) columnType).isFrozen())) {
      return String.format("full(%s)", columnName);
    }
    // Note: the keyword 'values' is not accepted as a valid index target function until 3.0
    return columnName;
  }

  // In C*>=3.0, index information is stored in a dedicated table:
  // CREATE TABLE system_schema.indexes (
  //   keyspace_name text,
  //   table_name text,
  //   index_name text,
  //   kind text,
  //   options frozen<map<text, text>>,
  //   PRIMARY KEY (keyspace_name, table_name, index_name)
  // ) WITH CLUSTERING ORDER BY (table_name ASC, index_name ASC)
  private DseIndexMetadata buildModernIndex(
      CqlIdentifier keyspaceId, CqlIdentifier tableId, AdminRow row) {
    CqlIdentifier name = CqlIdentifier.fromInternal(row.getString("index_name"));
    IndexKind kind = IndexKind.valueOf(row.getString("kind"));
    Map<String, String> options = row.getMapOfStringToString("options");
    String target = options.get("target");
    return new DefaultDseIndexMetadata(keyspaceId, tableId, name, kind, target, options);
  }

  private DseVertexMetadata buildVertex(
      CqlIdentifier tableId, Multimap<CqlIdentifier, AdminRow> keyspaceVertices) {

    if (keyspaceVertices == null) {
      return null;
    }
    Collection<AdminRow> tableVertices = keyspaceVertices.get(tableId);
    if (tableVertices == null || tableVertices.isEmpty()) {
      return null;
    }

    AdminRow row = tableVertices.iterator().next();
    return new DefaultDseVertexMetadata(getLabel(row));
  }

  private DseEdgeMetadata buildEdge(
      CqlIdentifier tableId,
      Multimap<CqlIdentifier, AdminRow> keyspaceEdges,
      Multimap<CqlIdentifier, AdminRow> keyspaceVertices) {

    if (keyspaceEdges == null) {
      return null;
    }

    Collection<AdminRow> tableEdges = keyspaceEdges.get(tableId);
    if (tableEdges == null || tableEdges.isEmpty()) {
      return null;
    }

    AdminRow row = tableEdges.iterator().next();

    CqlIdentifier fromTable = CqlIdentifier.fromInternal(row.getString("from_table"));

    CqlIdentifier toTable = CqlIdentifier.fromInternal(row.getString("to_table"));

    return new DefaultDseEdgeMetadata(
        getLabel(row),
        fromTable,
        findVertexLabel(fromTable, keyspaceVertices, "incoming"),
        CqlIdentifiers.wrapInternal(row.getListOfString("from_partition_key_columns")),
        CqlIdentifiers.wrapInternal(row.getListOfString("from_clustering_columns")),
        toTable,
        findVertexLabel(toTable, keyspaceVertices, "outgoing"),
        CqlIdentifiers.wrapInternal(row.getListOfString("to_partition_key_columns")),
        CqlIdentifiers.wrapInternal(row.getListOfString("to_clustering_columns")));
  }

  private CqlIdentifier getLabel(AdminRow row) {
    String rawLabel = row.getString("label_name");
    return (rawLabel == null || rawLabel.isEmpty()) ? null : CqlIdentifier.fromInternal(rawLabel);
  }

  // system_schema.edges only contains vertex table names. We also expose the labels in our metadata
  // objects, so we need to look them up in system_schema.vertices.
  private CqlIdentifier findVertexLabel(
      CqlIdentifier table,
      Multimap<CqlIdentifier, AdminRow> keyspaceVertices,
      String directionForErrorMessage) {
    Collection<AdminRow> tableVertices =
        (keyspaceVertices == null) ? null : keyspaceVertices.get(table);
    if (tableVertices == null || tableVertices.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Missing vertex definition for %s table %s",
              directionForErrorMessage, table.asCql(true)));
    }

    AdminRow row = tableVertices.iterator().next();
    return getLabel(row);
  }
}
