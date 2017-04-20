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
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.fail;

@RunWith(MockitoJUnitRunner.Silent.class)
public abstract class SchemaParserTestBase {

  protected static final CqlIdentifier KEYSPACE_ID = CqlIdentifier.fromInternal("ks");
  @Mock protected DefaultMetadata currentMetadata;
  @Mock protected InternalDriverContext context;

  protected static AdminRow mockFunctionRow(
      String keyspace,
      String name,
      List<String> argumentNames,
      List<String> argumentTypes,
      String body,
      boolean calledOnNullInput,
      String language,
      String returnType) {

    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.contains("keyspace_name")).thenReturn(true);
    Mockito.when(row.contains("function_name")).thenReturn(true);
    Mockito.when(row.contains("argument_names")).thenReturn(true);
    Mockito.when(row.contains("argument_types")).thenReturn(true);
    Mockito.when(row.contains("body")).thenReturn(true);
    Mockito.when(row.contains("called_on_null_input")).thenReturn(true);
    Mockito.when(row.contains("language")).thenReturn(true);
    Mockito.when(row.contains("return_type")).thenReturn(true);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspace);
    Mockito.when(row.getString("function_name")).thenReturn(name);
    Mockito.when(row.getListOfString("argument_names")).thenReturn(argumentNames);
    Mockito.when(row.getListOfString("argument_types")).thenReturn(argumentTypes);
    Mockito.when(row.getString("body")).thenReturn(body);
    Mockito.when(row.getBoolean("called_on_null_input")).thenReturn(calledOnNullInput);
    Mockito.when(row.getString("language")).thenReturn(language);
    Mockito.when(row.getString("return_type")).thenReturn(returnType);

    return row;
  }

  protected static AdminRow mockAggregateRow(
      String keyspace,
      String name,
      List<String> argumentTypes,
      String stateFunc,
      String stateType,
      String finalFunc,
      String returnType,
      Object initCond) {

    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.contains("keyspace_name")).thenReturn(true);
    Mockito.when(row.contains("aggregate_name")).thenReturn(true);
    Mockito.when(row.contains("argument_types")).thenReturn(true);
    Mockito.when(row.contains("state_func")).thenReturn(true);
    Mockito.when(row.contains("state_type")).thenReturn(true);
    Mockito.when(row.contains("final_func")).thenReturn(true);
    Mockito.when(row.contains("return_type")).thenReturn(true);
    Mockito.when(row.contains("initcond")).thenReturn(true);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspace);
    Mockito.when(row.getString("aggregate_name")).thenReturn(name);
    Mockito.when(row.getListOfString("argument_types")).thenReturn(argumentTypes);
    Mockito.when(row.getString("state_func")).thenReturn(stateFunc);
    Mockito.when(row.getString("state_type")).thenReturn(stateType);
    Mockito.when(row.getString("final_func")).thenReturn(finalFunc);
    Mockito.when(row.getString("return_type")).thenReturn(returnType);

    if (initCond instanceof ByteBuffer) {
      Mockito.when(row.isString("initcond")).thenReturn(false);
      Mockito.when(row.getByteBuffer("initcond")).thenReturn(((ByteBuffer) initCond));
    } else if (initCond instanceof String) {
      Mockito.when(row.isString("initcond")).thenReturn(true);
      Mockito.when(row.getString("initcond")).thenReturn(((String) initCond));
    } else {
      fail("Unsupported initcond type" + initCond.getClass());
    }

    return row;
  }

  protected static AdminRow mockTypeRow(
      String keyspace, String name, List<String> fieldNames, List<String> fieldTypes) {
    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspace);
    Mockito.when(row.getString("type_name")).thenReturn(name);
    Mockito.when(row.getListOfString("field_names")).thenReturn(fieldNames);
    Mockito.when(row.getListOfString("field_types")).thenReturn(fieldTypes);

    return row;
  }

  protected static AdminRow mockLegacyTableRow(String keyspace, String name, String comparator) {
    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspace);
    Mockito.when(row.getString("columnfamily_name")).thenReturn(name);
    Mockito.when(row.getBoolean("is_dense")).thenReturn(false);
    Mockito.when(row.getString("comparator")).thenReturn(comparator);
    Mockito.when(row.isString("caching")).thenReturn(true);
    Mockito.when(row.getString("caching"))
        .thenReturn("{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}");
    Mockito.when(row.getString("compaction_strategy_class"))
        .thenReturn("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy");
    Mockito.when(row.getString("compaction_strategy_options"))
        .thenReturn("{\"mock_option\":\"1\"}");

    return row;
  }

  protected static AdminRow mockLegacyColumnRow(
      String keyspaceName,
      String tableName,
      String name,
      String kind,
      String dataType,
      Integer position) {
    return mockLegacyColumnRow(
        keyspaceName, tableName, name, kind, dataType, position, null, null, null);
  }

  protected static AdminRow mockLegacyColumnRow(
      String keyspaceName,
      String tableName,
      String name,
      String kind,
      String dataType,
      int position,
      String indexName,
      String indexType,
      String indexOptions) {
    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.contains("validator")).thenReturn(true);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    Mockito.when(row.getString("columnfamily_name")).thenReturn(tableName);
    Mockito.when(row.getString("column_name")).thenReturn(name);
    Mockito.when(row.getString("type")).thenReturn(kind);
    Mockito.when(row.getString("validator")).thenReturn(dataType);
    Mockito.when(row.getInteger("component_index")).thenReturn(position);
    Mockito.when(row.getString("index_name")).thenReturn(indexName);
    Mockito.when(row.getString("index_type")).thenReturn(indexType);
    Mockito.when(row.getString("index_options")).thenReturn(indexOptions);

    return row;
  }

  protected static AdminRow mockModernTableRow(String keyspace, String name) {
    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.contains("flags")).thenReturn(true);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspace);
    Mockito.when(row.getString("table_name")).thenReturn(name);
    Mockito.when(row.getSetOfString("flags")).thenReturn(ImmutableSet.of("compound"));
    Mockito.when(row.isString("caching")).thenReturn(false);
    Mockito.when(row.get("caching", RelationParser.MAP_OF_TEXT_TO_TEXT))
        .thenReturn(ImmutableMap.of("keys", "ALL", "rows_per_partition", "NONE"));
    Mockito.when(row.get("compaction", RelationParser.MAP_OF_TEXT_TO_TEXT))
        .thenReturn(
            ImmutableMap.of(
                "class",
                "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
                "mock_option",
                "1"));

    return row;
  }

  protected static AdminRow mockModernColumnRow(
      String keyspaceName,
      String tableName,
      String name,
      String kind,
      String dataType,
      String clusteringOrder,
      Integer position) {
    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.contains("kind")).thenReturn(true);
    Mockito.when(row.contains("position")).thenReturn(true);
    Mockito.when(row.contains("clustering_order")).thenReturn(true);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    Mockito.when(row.getString("table_name")).thenReturn(tableName);
    Mockito.when(row.getString("column_name")).thenReturn(name);
    Mockito.when(row.getString("kind")).thenReturn(kind);
    Mockito.when(row.getString("type")).thenReturn(dataType);
    Mockito.when(row.getInteger("position")).thenReturn(position);
    Mockito.when(row.getString("clustering_order")).thenReturn(clusteringOrder);

    return row;
  }

  protected static AdminRow mockIndexRow(
      String keyspaceName,
      String tableName,
      String name,
      String kind,
      ImmutableMap<String, String> options) {
    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    Mockito.when(row.getString("table_name")).thenReturn(tableName);
    Mockito.when(row.getString("index_name")).thenReturn(name);
    Mockito.when(row.getString("kind")).thenReturn(kind);
    Mockito.when(row.getMapOfStringToString("options")).thenReturn(options);

    return row;
  }

  protected static AdminRow mockViewRow(
      String keyspaceName,
      String viewName,
      String baseTableName,
      boolean includeAllColumns,
      String whereClause) {
    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    Mockito.when(row.getString("view_name")).thenReturn(viewName);
    Mockito.when(row.getString("base_table_name")).thenReturn(baseTableName);
    Mockito.when(row.getBoolean("include_all_columns")).thenReturn(includeAllColumns);
    Mockito.when(row.getString("where_clause")).thenReturn(whereClause);

    return row;
  }

  protected static AdminRow mockModernKeyspaceRow(String keyspaceName) {
    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    Mockito.when(row.getBoolean("durable_writes")).thenReturn(true);

    Mockito.when(row.contains("strategy_class")).thenReturn(false);
    Mockito.when(row.getMapOfStringToString("replication"))
        .thenReturn(
            ImmutableMap.of(
                "class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1"));

    return row;
  }

  protected static AdminRow mockLegacyKeyspaceRow(String keyspaceName) {
    AdminRow row = Mockito.mock(AdminRow.class);

    Mockito.when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    Mockito.when(row.getBoolean("durable_writes")).thenReturn(true);

    Mockito.when(row.contains("strategy_class")).thenReturn(true);
    Mockito.when(row.getString("strategy_class"))
        .thenReturn("org.apache.cassandra.locator.SimpleStrategy");
    Mockito.when(row.getString("strategy_options")).thenReturn("{\"replication_factor\":\"1\"}");

    return row;
  }
}
