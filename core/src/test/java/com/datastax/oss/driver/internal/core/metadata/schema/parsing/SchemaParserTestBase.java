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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.KeyspaceFilter;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public abstract class SchemaParserTestBase {

  protected static final Node NODE_2_2 = mockNode(Version.V2_2_0);
  protected static final Node NODE_3_0 = mockNode(Version.V3_0_0);
  protected static final CqlIdentifier KEYSPACE_ID = CqlIdentifier.fromInternal("ks");
  @Mock protected DefaultMetadata currentMetadata;
  @Mock protected InternalDriverContext context;
  @Mock protected KeyspaceFilter keyspaceFilter;

  @Before
  public void setup() {
    when(keyspaceFilter.includes(anyString())).thenReturn(true);
  }

  protected static AdminRow mockFunctionRow(
      String keyspace,
      String name,
      List<String> argumentNames,
      List<String> argumentTypes,
      String body,
      boolean calledOnNullInput,
      String language,
      String returnType) {

    AdminRow row = mock(AdminRow.class);

    when(row.contains("keyspace_name")).thenReturn(true);
    when(row.contains("function_name")).thenReturn(true);
    when(row.contains("argument_names")).thenReturn(true);
    when(row.contains("argument_types")).thenReturn(true);
    when(row.contains("body")).thenReturn(true);
    when(row.contains("called_on_null_input")).thenReturn(true);
    when(row.contains("language")).thenReturn(true);
    when(row.contains("return_type")).thenReturn(true);

    when(row.getString("keyspace_name")).thenReturn(keyspace);
    when(row.getString("function_name")).thenReturn(name);
    when(row.getListOfString("argument_names")).thenReturn(argumentNames);
    when(row.getListOfString("argument_types")).thenReturn(argumentTypes);
    when(row.getString("body")).thenReturn(body);
    when(row.getBoolean("called_on_null_input")).thenReturn(calledOnNullInput);
    when(row.getString("language")).thenReturn(language);
    when(row.getString("return_type")).thenReturn(returnType);

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

    AdminRow row = mock(AdminRow.class);

    when(row.contains("keyspace_name")).thenReturn(true);
    when(row.contains("aggregate_name")).thenReturn(true);
    when(row.contains("argument_types")).thenReturn(true);
    when(row.contains("state_func")).thenReturn(true);
    when(row.contains("state_type")).thenReturn(true);
    when(row.contains("final_func")).thenReturn(true);
    when(row.contains("return_type")).thenReturn(true);
    when(row.contains("initcond")).thenReturn(true);

    when(row.getString("keyspace_name")).thenReturn(keyspace);
    when(row.getString("aggregate_name")).thenReturn(name);
    when(row.getListOfString("argument_types")).thenReturn(argumentTypes);
    when(row.getString("state_func")).thenReturn(stateFunc);
    when(row.getString("state_type")).thenReturn(stateType);
    when(row.getString("final_func")).thenReturn(finalFunc);
    when(row.getString("return_type")).thenReturn(returnType);

    if (initCond instanceof ByteBuffer) {
      when(row.isString("initcond")).thenReturn(false);
      when(row.getByteBuffer("initcond")).thenReturn(((ByteBuffer) initCond));
    } else if (initCond instanceof String) {
      when(row.isString("initcond")).thenReturn(true);
      when(row.getString("initcond")).thenReturn(((String) initCond));
    } else {
      fail("Unsupported initcond type" + initCond.getClass());
    }

    return row;
  }

  protected static AdminRow mockTypeRow(
      String keyspace, String name, List<String> fieldNames, List<String> fieldTypes) {
    AdminRow row = mock(AdminRow.class);

    when(row.getString("keyspace_name")).thenReturn(keyspace);
    when(row.getString("type_name")).thenReturn(name);
    when(row.getListOfString("field_names")).thenReturn(fieldNames);
    when(row.getListOfString("field_types")).thenReturn(fieldTypes);

    return row;
  }

  protected static AdminRow mockLegacyTableRow(String keyspace, String name, String comparator) {
    AdminRow row = mock(AdminRow.class);

    when(row.contains("table_name")).thenReturn(false);

    when(row.getString("keyspace_name")).thenReturn(keyspace);
    when(row.getString("columnfamily_name")).thenReturn(name);
    when(row.getBoolean("is_dense")).thenReturn(false);
    when(row.getString("comparator")).thenReturn(comparator);
    when(row.isString("caching")).thenReturn(true);
    when(row.getString("caching"))
        .thenReturn("{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}");
    when(row.getString("compaction_strategy_class"))
        .thenReturn("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy");
    when(row.getString("compaction_strategy_options")).thenReturn("{\"mock_option\":\"1\"}");

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
    AdminRow row = mock(AdminRow.class);

    when(row.contains("validator")).thenReturn(true);

    when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    when(row.getString("columnfamily_name")).thenReturn(tableName);
    when(row.getString("column_name")).thenReturn(name);
    when(row.getString("type")).thenReturn(kind);
    when(row.getString("validator")).thenReturn(dataType);
    when(row.getInteger("component_index")).thenReturn(position);
    when(row.getString("index_name")).thenReturn(indexName);
    when(row.getString("index_type")).thenReturn(indexType);
    when(row.getString("index_options")).thenReturn(indexOptions);

    return row;
  }

  protected static AdminRow mockModernTableRow(String keyspace, String name) {
    AdminRow row = mock(AdminRow.class);

    when(row.contains("flags")).thenReturn(true);
    when(row.contains("table_name")).thenReturn(true);

    when(row.getString("keyspace_name")).thenReturn(keyspace);
    when(row.getString("table_name")).thenReturn(name);
    when(row.getSetOfString("flags")).thenReturn(ImmutableSet.of("compound"));
    when(row.isString("caching")).thenReturn(false);
    when(row.get("caching", RelationParser.MAP_OF_TEXT_TO_TEXT))
        .thenReturn(ImmutableMap.of("keys", "ALL", "rows_per_partition", "NONE"));
    when(row.get("compaction", RelationParser.MAP_OF_TEXT_TO_TEXT))
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
    AdminRow row = mock(AdminRow.class);

    when(row.contains("kind")).thenReturn(true);
    when(row.contains("position")).thenReturn(true);
    when(row.contains("clustering_order")).thenReturn(true);

    when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    when(row.getString("table_name")).thenReturn(tableName);
    when(row.getString("column_name")).thenReturn(name);
    when(row.getString("kind")).thenReturn(kind);
    when(row.getString("type")).thenReturn(dataType);
    when(row.getInteger("position")).thenReturn(position);
    when(row.getString("clustering_order")).thenReturn(clusteringOrder);

    return row;
  }

  protected static AdminRow mockIndexRow(
      String keyspaceName,
      String tableName,
      String name,
      String kind,
      ImmutableMap<String, String> options) {
    AdminRow row = mock(AdminRow.class);

    when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    when(row.getString("table_name")).thenReturn(tableName);
    when(row.getString("index_name")).thenReturn(name);
    when(row.getString("kind")).thenReturn(kind);
    when(row.getMapOfStringToString("options")).thenReturn(options);

    return row;
  }

  protected static AdminRow mockViewRow(
      String keyspaceName,
      String viewName,
      String baseTableName,
      boolean includeAllColumns,
      String whereClause) {
    AdminRow row = mock(AdminRow.class);

    when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    when(row.getString("view_name")).thenReturn(viewName);
    when(row.getString("base_table_name")).thenReturn(baseTableName);
    when(row.getBoolean("include_all_columns")).thenReturn(includeAllColumns);
    when(row.getString("where_clause")).thenReturn(whereClause);

    return row;
  }

  protected static AdminRow mockModernKeyspaceRow(String keyspaceName) {
    AdminRow row = mock(AdminRow.class);

    when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    when(row.getBoolean("durable_writes")).thenReturn(true);

    when(row.contains("strategy_class")).thenReturn(false);
    when(row.getMapOfStringToString("replication"))
        .thenReturn(
            ImmutableMap.of(
                "class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1"));

    return row;
  }

  protected static AdminRow mockLegacyKeyspaceRow(String keyspaceName) {
    AdminRow row = mock(AdminRow.class);

    when(row.getString("keyspace_name")).thenReturn(keyspaceName);
    when(row.getBoolean("durable_writes")).thenReturn(true);

    when(row.contains("strategy_class")).thenReturn(true);
    when(row.getString("strategy_class")).thenReturn("org.apache.cassandra.locator.SimpleStrategy");
    when(row.getString("strategy_options")).thenReturn("{\"replication_factor\":\"1\"}");

    return row;
  }

  private static Node mockNode(Version version) {
    Node node = mock(Node.class);
    when(node.getExtras()).thenReturn(Collections.emptyMap());
    when(node.getCassandraVersion()).thenReturn(version);
    return node;
  }
}
