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
package com.datastax.oss.driver.api.querybuilder.schema;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.alterTable;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Test;

public class AlterTableTest {

  @Test
  public void should_not_throw_on_toString_for_AlterTableStart() {
    assertThat(alterTable("foo").toString()).isEqualTo("ALTER TABLE foo");
  }

  @Test
  public void should_generate_alter_table_with_alter_column_type() {
    assertThat(alterTable("foo", "bar").alterColumn("x", DataTypes.TEXT))
        .hasCql("ALTER TABLE foo.bar ALTER x TYPE text");
  }

  @Test
  public void should_generate_alter_table_with_add_single_column() {
    assertThat(alterTable("foo", "bar").addColumn("x", DataTypes.TEXT))
        .hasCql("ALTER TABLE foo.bar ADD x text");
  }

  @Test
  public void should_generate_alter_table_with_add_single_column_static() {
    assertThat(alterTable("foo", "bar").addStaticColumn("x", DataTypes.TEXT))
        .hasCql("ALTER TABLE foo.bar ADD x text STATIC");
  }

  @Test
  public void should_generate_alter_table_with_add_three_columns() {
    assertThat(
            alterTable("foo", "bar")
                .addColumn("x", DataTypes.TEXT)
                .addStaticColumn("y", DataTypes.FLOAT)
                .addColumn("z", DataTypes.DOUBLE))
        .hasCql("ALTER TABLE foo.bar ADD (x text,y float STATIC,z double)");
  }

  @Test
  public void should_generate_alter_table_with_drop_single_column() {
    assertThat(alterTable("foo", "bar").dropColumn("x")).hasCql("ALTER TABLE foo.bar DROP x");
  }

  @Test
  public void should_generate_alter_table_with_drop_two_columns() {
    assertThat(alterTable("foo", "bar").dropColumn("x").dropColumn("y"))
        .hasCql("ALTER TABLE foo.bar DROP (x,y)");
  }

  @Test
  public void should_generate_alter_table_with_drop_two_columns_at_once() {
    assertThat(alterTable("foo", "bar").dropColumns("x", "y"))
        .hasCql("ALTER TABLE foo.bar DROP (x,y)");
  }

  @Test
  public void should_generate_alter_table_with_rename_single_column() {
    assertThat(alterTable("foo", "bar").renameColumn("x", "y"))
        .hasCql("ALTER TABLE foo.bar RENAME x TO y");
  }

  @Test
  public void should_generate_alter_table_with_rename_three_columns() {
    assertThat(
            alterTable("foo", "bar")
                .renameColumn("x", "y")
                .renameColumn("u", "v")
                .renameColumn("b", "a"))
        .hasCql("ALTER TABLE foo.bar RENAME x TO y AND u TO v AND b TO a");
  }

  @Test
  public void should_generate_alter_table_with_drop_compact_storage() {
    assertThat(alterTable("bar").dropCompactStorage())
        .hasCql("ALTER TABLE bar DROP COMPACT STORAGE");
  }

  @Test
  public void should_generate_alter_table_with_options() {
    assertThat(alterTable("bar").withComment("Hello").withCDC(true))
        .hasCql("ALTER TABLE bar WITH comment='Hello' AND cdc=true");
  }

  @Test
  public void should_generate_alter_table_with_no_compression() {
    assertThat(alterTable("bar").withNoCompression())
        .hasCql("ALTER TABLE bar WITH compression={'sstable_compression':''}");
  }
}
