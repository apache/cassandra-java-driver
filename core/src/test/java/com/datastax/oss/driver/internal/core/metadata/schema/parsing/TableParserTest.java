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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexKind;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.CassandraSchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.junit.Test;

public class TableParserTest extends SchemaParserTestBase {

  private static final AdminRow TABLE_ROW_2_2 =
      mockLegacyTableRow(
          "ks",
          "foo",
          "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type)");
  private static final ImmutableList<AdminRow> COLUMN_ROWS_2_2 =
      ImmutableList.of(
          mockLegacyColumnRow(
              "ks", "foo", "k2", "partition_key", "org.apache.cassandra.db.marshal.UTF8Type", 1),
          mockLegacyColumnRow(
              "ks", "foo", "k1", "partition_key", "org.apache.cassandra.db.marshal.Int32Type", 0),
          mockLegacyColumnRow(
              "ks", "foo", "cc1", "clustering_key", "org.apache.cassandra.db.marshal.Int32Type", 0),
          mockLegacyColumnRow(
              "ks",
              "foo",
              "cc2",
              "clustering_key",
              "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)",
              1),
          mockLegacyColumnRow(
              "ks",
              "foo",
              "v",
              "regular",
              "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)",
              -1,
              "foo_v_idx",
              "COMPOSITES",
              "{}"));

  static final AdminRow TABLE_ROW_3_0 = mockModernTableRow("ks", "foo");
  static final ImmutableList<AdminRow> COLUMN_ROWS_3_0 =
      ImmutableList.of(
          mockModernColumnRow("ks", "foo", "k2", "partition_key", "text", "none", 1),
          mockModernColumnRow("ks", "foo", "k1", "partition_key", "int", "none", 0),
          mockModernColumnRow("ks", "foo", "cc1", "clustering", "int", "asc", 0),
          mockModernColumnRow("ks", "foo", "cc2", "clustering", "int", "desc", 1),
          mockModernColumnRow("ks", "foo", "v", "regular", "int", "none", -1));
  static final ImmutableList<AdminRow> INDEX_ROWS_3_0 =
      ImmutableList.of(
          mockIndexRow("ks", "foo", "foo_v_idx", "COMPOSITES", ImmutableMap.of("target", "v")));

  @Test
  public void should_skip_when_no_column_rows() {
    SchemaRows rows = legacyRows(TABLE_ROW_2_2, Collections.emptyList());
    TableParser parser = new TableParser(rows, context);
    TableMetadata table = parser.parseTable(TABLE_ROW_2_2, KEYSPACE_ID, Collections.emptyMap());

    assertThat(table).isNull();
  }

  @Test
  public void should_parse_legacy_tables() {
    SchemaRows rows = legacyRows(TABLE_ROW_2_2, COLUMN_ROWS_2_2);
    TableParser parser = new TableParser(rows, context);
    TableMetadata table = parser.parseTable(TABLE_ROW_2_2, KEYSPACE_ID, Collections.emptyMap());

    checkTable(table);

    assertThat(table.getOptions().get(CqlIdentifier.fromInternal("caching")))
        .isEqualTo("{\"keys\":\"ALL\", \"rows_per_partition\":\"NONE\"}");
  }

  @Test
  public void should_parse_modern_tables() {
    SchemaRows rows = modernRows(TABLE_ROW_3_0, COLUMN_ROWS_3_0, INDEX_ROWS_3_0);
    TableParser parser = new TableParser(rows, context);
    TableMetadata table = parser.parseTable(TABLE_ROW_3_0, KEYSPACE_ID, Collections.emptyMap());

    checkTable(table);

    @SuppressWarnings("unchecked")
    Map<String, String> caching =
        (Map<String, String>) table.getOptions().get(CqlIdentifier.fromInternal("caching"));
    assertThat(caching)
        .hasSize(2)
        .containsEntry("keys", "ALL")
        .containsEntry("rows_per_partition", "NONE");
  }

  /** Covers two additional Cassandra 4.0 options added in JAVA-2090. */
  @Test
  public void should_parse_read_repair_and_additional_write_policy() {
    AdminRow tableRow40 = mockModernTableRow("ks", "foo");
    when(tableRow40.get("read_repair", TypeCodecs.TEXT)).thenReturn("NONE");
    when(tableRow40.get("additional_write_policy", TypeCodecs.TEXT)).thenReturn("40p");

    SchemaRows rows = modernRows(tableRow40, COLUMN_ROWS_3_0, INDEX_ROWS_3_0);
    TableParser parser = new TableParser(rows, context);
    TableMetadata table = parser.parseTable(tableRow40, KEYSPACE_ID, Collections.emptyMap());

    checkTable(table);

    assertThat(table.getOptions())
        .containsEntry(CqlIdentifier.fromInternal("read_repair"), "NONE")
        .containsEntry(CqlIdentifier.fromInternal("additional_write_policy"), "40p");
  }

  // Shared between 2.2 and 3.0 tests, all expected values are the same except the 'caching' option
  private void checkTable(TableMetadata table) {
    assertThat(table.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(table.getName().asInternal()).isEqualTo("foo");

    assertThat(table.getPartitionKey()).hasSize(2);
    ColumnMetadata pk0 = table.getPartitionKey().get(0);
    assertThat(pk0.getName().asInternal()).isEqualTo("k1");
    assertThat(pk0.getType()).isEqualTo(DataTypes.INT);
    ColumnMetadata pk1 = table.getPartitionKey().get(1);
    assertThat(pk1.getName().asInternal()).isEqualTo("k2");
    assertThat(pk1.getType()).isEqualTo(DataTypes.TEXT);

    assertThat(table.getClusteringColumns().entrySet()).hasSize(2);
    Iterator<ColumnMetadata> clusteringColumnsIterator =
        table.getClusteringColumns().keySet().iterator();
    ColumnMetadata clusteringColumn1 = clusteringColumnsIterator.next();
    assertThat(clusteringColumn1.getName().asInternal()).isEqualTo("cc1");
    ColumnMetadata clusteringColumn2 = clusteringColumnsIterator.next();
    assertThat(clusteringColumn2.getName().asInternal()).isEqualTo("cc2");
    assertThat(table.getClusteringColumns().values())
        .containsExactly(ClusteringOrder.ASC, ClusteringOrder.DESC);

    assertThat(table.getColumns())
        .containsOnlyKeys(
            CqlIdentifier.fromInternal("k1"),
            CqlIdentifier.fromInternal("k2"),
            CqlIdentifier.fromInternal("cc1"),
            CqlIdentifier.fromInternal("cc2"),
            CqlIdentifier.fromInternal("v"));
    ColumnMetadata regularColumn = table.getColumns().get(CqlIdentifier.fromInternal("v"));
    assertThat(regularColumn.getName().asInternal()).isEqualTo("v");
    assertThat(regularColumn.getType()).isEqualTo(DataTypes.INT);

    assertThat(table.getIndexes()).containsOnlyKeys(CqlIdentifier.fromInternal("foo_v_idx"));
    IndexMetadata index = table.getIndexes().get(CqlIdentifier.fromInternal("foo_v_idx"));
    assertThat(index.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(index.getTable().asInternal()).isEqualTo("foo");
    assertThat(index.getName().asInternal()).isEqualTo("foo_v_idx");
    assertThat(index.getClassName()).isNotPresent();
    assertThat(index.getKind()).isEqualTo(IndexKind.COMPOSITES);
    assertThat(index.getTarget()).isEqualTo("v");

    assertThat(table.getIndex("foo_v_idx")).hasValue(index);

    @SuppressWarnings("unchecked")
    Map<String, String> compaction =
        (Map<String, String>) table.getOptions().get(CqlIdentifier.fromInternal("compaction"));
    assertThat(compaction)
        .hasSize(2)
        .containsEntry("class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy")
        .containsEntry("mock_option", "1");
  }

  private SchemaRows legacyRows(AdminRow tableRow, Iterable<AdminRow> columnRows) {
    return rows(tableRow, columnRows, null, NODE_2_2);
  }

  private SchemaRows modernRows(
      AdminRow tableRow, Iterable<AdminRow> columnRows, Iterable<AdminRow> indexesRows) {
    return rows(tableRow, columnRows, indexesRows, NODE_3_0);
  }

  private SchemaRows rows(
      AdminRow tableRow, Iterable<AdminRow> columnRows, Iterable<AdminRow> indexesRows, Node node) {
    CassandraSchemaRows.Builder builder =
        new CassandraSchemaRows.Builder(node, keyspaceFilter, "test")
            .withTables(ImmutableList.of(tableRow))
            .withColumns(columnRows);
    if (indexesRows != null) {
      builder.withIndexes(indexesRows);
    }
    return builder.build();
  }
}
