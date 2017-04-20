/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultViewMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ViewParser extends RelationParser {

  private static final Logger LOG = LoggerFactory.getLogger(ViewParser.class);

  ViewParser(SchemaRows rows, DataTypeParser dataTypeParser, InternalDriverContext context) {
    super(rows, dataTypeParser, context);
  }

  ViewMetadata parseView(
      AdminRow viewRow, CqlIdentifier keyspaceId, Map<CqlIdentifier, UserDefinedType> userTypes) {
    // Cassandra 3.0 (no views in earlier versions):
    // CREATE TABLE system_schema.views (
    //     keyspace_name text,
    //     view_name text,
    //     base_table_id uuid,
    //     base_table_name text,
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
    //     gc_grace_seconds int,
    //     id uuid,
    //     include_all_columns boolean,
    //     max_index_interval int,
    //     memtable_flush_period_in_ms int,
    //     min_index_interval int,
    //     read_repair_chance double,
    //     speculative_retry text,
    //     where_clause text,
    //     PRIMARY KEY (keyspace_name, view_name)
    // ) WITH CLUSTERING ORDER BY (view_name ASC)
    CqlIdentifier viewId = CqlIdentifier.fromInternal(viewRow.getString("view_name"));

    UUID uuid = viewRow.getUuid("id");
    CqlIdentifier baseTableId = CqlIdentifier.fromInternal(viewRow.getString("base_table_name"));
    boolean includesAllColumns =
        MoreObjects.firstNonNull(viewRow.getBoolean("include_all_columns"), false);
    String whereClause = viewRow.getString("where_clause");

    List<RawColumn> rawColumns =
        RawColumn.toRawColumns(
            rows.columns.getOrDefault(keyspaceId, ImmutableMultimap.of()).get(viewId),
            keyspaceId,
            userTypes);
    if (rawColumns.isEmpty()) {
      LOG.warn(
          "[{}] Processing VIEW refresh for {}.{} but found no matching rows, skipping",
          logPrefix,
          keyspaceId,
          viewId);
      return null;
    }

    Collections.sort(rawColumns);
    ImmutableMap.Builder<CqlIdentifier, ColumnMetadata> allColumnsBuilder = ImmutableMap.builder();
    ImmutableList.Builder<ColumnMetadata> partitionKeyBuilder = ImmutableList.builder();
    ImmutableMap.Builder<ColumnMetadata, ClusteringOrder> clusteringColumnsBuilder =
        ImmutableMap.builder();

    for (RawColumn raw : rawColumns) {
      DataType dataType = dataTypeParser.parse(keyspaceId, raw.dataType, userTypes, context);
      ColumnMetadata column =
          new DefaultColumnMetadata(
              keyspaceId, viewId, raw.name, dataType, raw.kind == RawColumn.Kind.STATIC);
      switch (raw.kind) {
        case PARTITION_KEY:
          partitionKeyBuilder.add(column);
          break;
        case CLUSTERING_COLUMN:
          clusteringColumnsBuilder.put(
              column, raw.reversed ? ClusteringOrder.DESC : ClusteringOrder.ASC);
          break;
      }
      allColumnsBuilder.put(column.getName(), column);
    }

    Map<CqlIdentifier, Object> options;
    try {
      options = parseOptions(viewRow);
    } catch (Exception e) {
      // Options change the most often, so be especially lenient if anything goes wrong.
      LOG.warn(
          "[{}] Error while parsing options for {}.{}, getOptions() will be empty",
          logPrefix,
          keyspaceId,
          viewId,
          e);
      options = Collections.emptyMap();
    }

    return new DefaultViewMetadata(
        keyspaceId,
        viewId,
        baseTableId,
        includesAllColumns,
        whereClause,
        uuid,
        partitionKeyBuilder.build(),
        clusteringColumnsBuilder.build(),
        allColumnsBuilder.build(),
        options);
  }
}
