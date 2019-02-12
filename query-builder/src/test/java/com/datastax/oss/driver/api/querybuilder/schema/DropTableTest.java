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
package com.datastax.oss.driver.api.querybuilder.schema;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropTable;

import org.junit.Test;

public class DropTableTest {

  @Test
  public void should_generate_drop_table() {
    assertThat(dropTable("bar")).hasCql("DROP TABLE bar");
  }

  @Test
  public void should_generate_drop_table_with_keyspace() {
    assertThat(dropTable("foo", "bar")).hasCql("DROP TABLE foo.bar");
  }

  @Test
  public void should_generate_drop_table_if_exists() {
    assertThat(dropTable("bar").ifExists()).hasCql("DROP TABLE IF EXISTS bar");
  }
}
