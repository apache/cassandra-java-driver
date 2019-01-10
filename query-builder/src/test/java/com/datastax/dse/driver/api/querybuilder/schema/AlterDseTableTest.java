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
package com.datastax.dse.driver.api.querybuilder.schema;

import static com.datastax.dse.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.dse.driver.api.querybuilder.DseSchemaBuilder.alterDseTable;
import static com.datastax.dse.driver.api.querybuilder.schema.DseGraphEdgeSide.table;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Test;

public class AlterDseTableTest {

  @Test
  public void should_not_throw_on_toString_for_AlterTableStart() {
    assertThat(alterDseTable("foo").toString()).isEqualTo("ALTER TABLE foo");
  }

  @Test
  public void should_generate_alter_table_with_alter_column_type() {
    assertThat(alterDseTable("foo", "bar").alterColumn("x", DataTypes.TEXT))
        .hasCql("ALTER TABLE foo.bar ALTER x TYPE text");
  }

  @Test
  public void should_generate_alter_table_with_add_single_column() {
    assertThat(alterDseTable("foo", "bar").addColumn("x", DataTypes.TEXT))
        .hasCql("ALTER TABLE foo.bar ADD x text");
  }

  @Test
  public void should_generate_alter_table_with_add_three_columns() {
    assertThat(
            alterDseTable("foo", "bar")
                .addColumn("x", DataTypes.TEXT)
                .addStaticColumn("y", DataTypes.FLOAT)
                .addColumn("z", DataTypes.DOUBLE))
        .hasCql("ALTER TABLE foo.bar ADD (x text,y float STATIC,z double)");
  }

  @Test
  public void should_generate_alter_table_with_drop_single_column() {
    assertThat(alterDseTable("foo", "bar").dropColumn("x")).hasCql("ALTER TABLE foo.bar DROP x");
  }

  @Test
  public void should_generate_alter_table_with_drop_two_columns() {
    assertThat(alterDseTable("foo", "bar").dropColumn("x").dropColumn("y"))
        .hasCql("ALTER TABLE foo.bar DROP (x,y)");
  }

  @Test
  public void should_generate_alter_table_with_drop_two_columns_at_once() {
    assertThat(alterDseTable("foo", "bar").dropColumns("x", "y"))
        .hasCql("ALTER TABLE foo.bar DROP (x,y)");
  }

  @Test
  public void should_generate_alter_table_with_rename_single_column() {
    assertThat(alterDseTable("foo", "bar").renameColumn("x", "y"))
        .hasCql("ALTER TABLE foo.bar RENAME x TO y");
  }

  @Test
  public void should_generate_alter_table_with_rename_three_columns() {
    assertThat(
            alterDseTable("foo", "bar")
                .renameColumn("x", "y")
                .renameColumn("u", "v")
                .renameColumn("b", "a"))
        .hasCql("ALTER TABLE foo.bar RENAME x TO y AND u TO v AND b TO a");
  }

  @Test
  public void should_generate_alter_table_with_drop_compact_storage() {
    assertThat(alterDseTable("bar").dropCompactStorage())
        .hasCql("ALTER TABLE bar DROP COMPACT STORAGE");
  }

  @Test
  public void should_generate_alter_table_with_options() {
    assertThat(alterDseTable("bar").withComment("Hello").withCDC(true))
        .hasCql("ALTER TABLE bar WITH comment='Hello' AND cdc=true");
  }

  @Test
  public void should_generate_alter_table_with_no_compression() {
    assertThat(alterDseTable("bar").withNoCompression())
        .hasCql("ALTER TABLE bar WITH compression={'sstable_compression':''}");
  }

  @Test
  public void should_generate_alter_table_to_add_anonymous_vertex_label() {
    assertThat(alterDseTable("bar").withVertexLabel()).hasCql("ALTER TABLE bar WITH VERTEX LABEL");
  }

  @Test
  public void should_generate_alter_table_to_add_named_vertex_label() {
    assertThat(alterDseTable("bar").withVertexLabel("baz"))
        .hasCql("ALTER TABLE bar WITH VERTEX LABEL baz");
  }

  @Test
  public void should_generate_alter_table_to_remove_anonymous_vertex_label() {
    assertThat(alterDseTable("bar").withoutVertexLabel())
        .hasCql("ALTER TABLE bar WITHOUT VERTEX LABEL");
  }

  @Test
  public void should_generate_alter_table_to_remove_named_vertex_label() {
    assertThat(alterDseTable("bar").withoutVertexLabel("baz"))
        .hasCql("ALTER TABLE bar WITHOUT VERTEX LABEL baz");
  }

  @Test
  public void should_generate_alter_table_to_add_anonymous_edge_label() {
    assertThat(
            alterDseTable("bar")
                .withEdgeLabel(
                    table("source").withPartitionKey("pk"),
                    table("dest")
                        .withPartitionKey("pk1")
                        .withPartitionKey("pk2")
                        .withClusteringColumn("cc")))
        .hasCql("ALTER TABLE bar WITH EDGE LABEL FROM source(pk) TO dest((pk1,pk2),cc)");
  }

  @Test
  public void should_generate_alter_table_to_add_named_edge_label() {
    assertThat(
            alterDseTable("bar")
                .withEdgeLabel(
                    "e",
                    table("source").withPartitionKey("pk"),
                    table("dest")
                        .withPartitionKey("pk1")
                        .withPartitionKey("pk2")
                        .withClusteringColumn("cc")))
        .hasCql("ALTER TABLE bar WITH EDGE LABEL e FROM source(pk) TO dest((pk1,pk2),cc)");
  }

  @Test
  public void should_generate_alter_table_to_remove_anonymous_edge_label() {
    assertThat(alterDseTable("bar").withoutEdgeLabel())
        .hasCql("ALTER TABLE bar WITHOUT EDGE LABEL");
  }

  @Test
  public void should_generate_alter_table_to_remove_named_edge_label() {
    assertThat(alterDseTable("bar").withoutEdgeLabel("baz"))
        .hasCql("ALTER TABLE bar WITHOUT EDGE LABEL baz");
  }
}
