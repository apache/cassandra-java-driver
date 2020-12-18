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

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.metadata.MetadataRefresh;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.CassandraSchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SchemaRefresh;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.Test;

public class SchemaParserTest extends SchemaParserTestBase {

  @Test
  public void should_parse_modern_keyspace_row() {
    SchemaRefresh refresh =
        (SchemaRefresh)
            parse(rows -> rows.withKeyspaces(ImmutableList.of(mockModernKeyspaceRow("ks"))));

    assertThat(refresh.newKeyspaces).hasSize(1);
    KeyspaceMetadata keyspace = refresh.newKeyspaces.values().iterator().next();
    checkKeyspace(keyspace);
  }

  @Test
  public void should_parse_legacy_keyspace_row() {
    SchemaRefresh refresh =
        (SchemaRefresh)
            parse(rows -> rows.withKeyspaces(ImmutableList.of(mockLegacyKeyspaceRow("ks"))));

    assertThat(refresh.newKeyspaces).hasSize(1);
    KeyspaceMetadata keyspace = refresh.newKeyspaces.values().iterator().next();
    checkKeyspace(keyspace);
  }

  @Test
  public void should_parse_keyspace_with_all_children() {
    // Needed to parse the aggregate
    when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);

    SchemaRefresh refresh =
        (SchemaRefresh)
            parse(
                rows ->
                    rows.withKeyspaces(ImmutableList.of(mockModernKeyspaceRow("ks")))
                        .withTypes(
                            ImmutableList.of(
                                mockTypeRow(
                                    "ks", "t", ImmutableList.of("i"), ImmutableList.of("int"))))
                        .withTables(ImmutableList.of(TableParserTest.TABLE_ROW_3_0))
                        .withColumns(TableParserTest.COLUMN_ROWS_3_0)
                        .withIndexes(TableParserTest.INDEX_ROWS_3_0)
                        .withViews(ImmutableList.of(ViewParserTest.VIEW_ROW_3_0))
                        .withColumns(ViewParserTest.COLUMN_ROWS_3_0)
                        .withFunctions(ImmutableList.of(FunctionParserTest.ID_ROW_3_0))
                        .withAggregates(
                            ImmutableList.of(AggregateParserTest.SUM_AND_TO_STRING_ROW_3_0)));

    assertThat(refresh.newKeyspaces).hasSize(1);
    KeyspaceMetadata keyspace = refresh.newKeyspaces.values().iterator().next();
    checkKeyspace(keyspace);

    assertThat(keyspace.getUserDefinedTypes())
        .hasSize(1)
        .containsKey(CqlIdentifier.fromInternal("t"));
    assertThat(keyspace.getTables()).hasSize(1).containsKey(CqlIdentifier.fromInternal("foo"));
    assertThat(keyspace.getViews())
        .hasSize(1)
        .containsKey(CqlIdentifier.fromInternal("alltimehigh"));
    assertThat(keyspace.getFunctions())
        .hasSize(1)
        .containsKey(new FunctionSignature(CqlIdentifier.fromInternal("id"), DataTypes.INT));
    assertThat(keyspace.getAggregates())
        .hasSize(1)
        .containsKey(
            new FunctionSignature(CqlIdentifier.fromInternal("sum_and_to_string"), DataTypes.INT));
  }

  // Common assertions, the keyspace has the same info in all of our single keyspace examples
  private void checkKeyspace(KeyspaceMetadata keyspace) {
    assertThat(keyspace.getName().asInternal()).isEqualTo("ks");
    assertThat(keyspace.isDurableWrites()).isTrue();
    assertThat(keyspace.getReplication())
        .hasSize(2)
        .containsEntry("class", "org.apache.cassandra.locator.SimpleStrategy")
        .containsEntry("replication_factor", "1");
  }

  @Test
  public void should_parse_multiple_keyspaces() {
    SchemaRefresh refresh =
        (SchemaRefresh)
            parse(
                rows ->
                    rows.withKeyspaces(
                            ImmutableList.of(
                                mockModernKeyspaceRow("ks1"), mockModernKeyspaceRow("ks2")))
                        .withTypes(
                            ImmutableList.of(
                                mockTypeRow(
                                    "ks1", "t1", ImmutableList.of("i"), ImmutableList.of("int")),
                                mockTypeRow(
                                    "ks2", "t2", ImmutableList.of("i"), ImmutableList.of("int")))));

    Map<CqlIdentifier, KeyspaceMetadata> keyspaces = refresh.newKeyspaces;
    assertThat(keyspaces).hasSize(2);
    KeyspaceMetadata ks1 = keyspaces.get(CqlIdentifier.fromInternal("ks1"));
    KeyspaceMetadata ks2 = keyspaces.get(CqlIdentifier.fromInternal("ks2"));

    assertThat(ks1.getName().asInternal()).isEqualTo("ks1");
    assertThat(ks1.getUserDefinedTypes()).hasSize(1).containsKey(CqlIdentifier.fromInternal("t1"));
    assertThat(ks2.getName().asInternal()).isEqualTo("ks2");
    assertThat(ks2.getUserDefinedTypes()).hasSize(1).containsKey(CqlIdentifier.fromInternal("t2"));
  }

  private MetadataRefresh parse(Consumer<CassandraSchemaRows.Builder> builderConfig) {
    CassandraSchemaRows.Builder builder =
        new CassandraSchemaRows.Builder(NODE_3_0, keyspaceFilter, "test");
    builderConfig.accept(builder);
    SchemaRows rows = builder.build();
    return new CassandraSchemaParser(rows, context).parse();
  }
}
