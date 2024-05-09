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
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.alterType;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Test;

public class AlterTypeTest {

  @Test
  public void should_not_throw_on_toString_for_AlterTypeStart() {
    assertThat(alterType("foo").toString()).isEqualTo("ALTER TYPE foo");
  }

  @Test
  public void should_generate_alter_type_with_alter_field_type() {
    assertThat(alterType("foo", "bar").alterField("x", DataTypes.TEXT))
        .hasCql("ALTER TYPE foo.bar ALTER x TYPE text");
  }

  @Test
  public void should_generate_alter_table_with_add_field() {
    assertThat(alterType("foo", "bar").addField("x", DataTypes.TEXT))
        .hasCql("ALTER TYPE foo.bar ADD x text");
  }

  @Test
  public void should_generate_alter_table_with_rename_single_column() {
    assertThat(alterType("foo", "bar").renameField("x", "y"))
        .hasCql("ALTER TYPE foo.bar RENAME x TO y");
  }

  @Test
  public void should_generate_alter_table_with_rename_three_columns() {
    assertThat(alterType("bar").renameField("x", "y").renameField("u", "v").renameField("b", "a"))
        .hasCql("ALTER TYPE bar RENAME x TO y AND u TO v AND b TO a");
  }
}
