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
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createIndex;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import org.junit.Test;

public class CreateIndexTest {

  @Test
  public void should_not_throw_on_toString_for_CreateIndexStart() {
    assertThat(createIndex().toString()).isEqualTo("CREATE INDEX");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateIndexOnTable() {
    assertThat(createIndex().onTable("x").toString()).isEqualTo("CREATE INDEX ON x");
  }

  @Test
  public void should_generate_create_index_with_no_name() {
    assertThat(createIndex().onTable("x").andColumn("y")).hasCql("CREATE INDEX ON x (y)");
  }

  @Test
  public void should_generate_create_custom_index_with_no_name() {
    assertThat(createIndex().custom("MyClass").onTable("x").andColumn("y"))
        .hasCql("CREATE CUSTOM INDEX ON x (y) USING 'MyClass'");
  }

  @Test
  public void should_generate_create_custom_index_if_not_exists_with_no_name() {
    assertThat(createIndex().custom("MyClass").ifNotExists().onTable("x").andColumn("y"))
        .hasCql("CREATE CUSTOM INDEX IF NOT EXISTS ON x (y) USING 'MyClass'");
  }

  @Test
  public void should_generate_create_index_with_no_name_if_not_exists() {
    assertThat(createIndex().ifNotExists().onTable("x").andColumn("y"))
        .hasCql("CREATE INDEX IF NOT EXISTS ON x (y)");
  }

  @Test
  public void should_generate_custom_index_with_name() {
    assertThat(createIndex("bar").custom("MyClass").onTable("x").andColumn("y"))
        .hasCql("CREATE CUSTOM INDEX bar ON x (y) USING 'MyClass'");
  }

  @Test
  public void should_generate_create_custom_index_if_not_exists_with_name() {
    assertThat(createIndex("bar").custom("MyClass").ifNotExists().onTable("x").andColumn("y"))
        .hasCql("CREATE CUSTOM INDEX IF NOT EXISTS bar ON x (y) USING 'MyClass'");
  }

  @Test
  public void should_generate_index_with_keyspace() {
    assertThat(createIndex("bar").onTable("foo", "x").andColumn("y"))
        .hasCql("CREATE INDEX bar ON foo.x (y)");
  }

  @Test
  public void should_generate_create_index_with_name_if_not_exists() {
    assertThat(createIndex("bar").ifNotExists().onTable("x").andColumn("y"))
        .hasCql("CREATE INDEX IF NOT EXISTS bar ON x (y)");
  }

  @Test
  public void should_generate_create_index_values() {
    assertThat(createIndex().onTable("x").andColumnValues("m"))
        .hasCql("CREATE INDEX ON x (VALUES(m))");
  }

  @Test
  public void should_generate_create_index_keys() {
    assertThat(createIndex().onTable("x").andColumnKeys("m")).hasCql("CREATE INDEX ON x (KEYS(m))");
  }

  @Test
  public void should_generate_create_index_entries() {
    assertThat(createIndex().onTable("x").andColumnEntries("m"))
        .hasCql("CREATE INDEX ON x (ENTRIES(m))");
  }

  @Test
  public void should_generate_create_index_full() {
    assertThat(createIndex().onTable("x").andColumnFull("l")).hasCql("CREATE INDEX ON x (FULL(l))");
  }

  @Test
  public void should_generate_create_index_custom_index_type() {
    assertThat(createIndex().onTable("x").andColumn("m", "CUST"))
        .hasCql("CREATE INDEX ON x (CUST(m))");
  }

  @Test
  public void should_generate_create_index_with_options() {
    assertThat(
            createIndex()
                .custom("MyClass")
                .onTable("x")
                .andColumn("y")
                .withOption("opt1", 1)
                .withOption("opt2", "data"))
        .hasCql("CREATE CUSTOM INDEX ON x (y) USING 'MyClass' WITH opt1=1 AND opt2='data'");
  }

  @Test
  public void should_generate_create_custom_index_with_options() {
    assertThat(
            createIndex()
                .onTable("x")
                .andColumn("y")
                .withOption("opt1", 1)
                .withOption("opt2", "data"))
        .hasCql("CREATE INDEX ON x (y) WITH opt1=1 AND opt2='data'");
  }

  @Test
  public void should_generate_create_index_sasi_with_options() {
    assertThat(
            createIndex()
                .usingSASI()
                .onTable("x")
                .andColumn("y")
                .withSASIOptions(ImmutableMap.of("mode", "CONTAINS", "tokenization_locale", "en")))
        .hasCql(
            "CREATE CUSTOM INDEX ON x (y) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS={'mode':'CONTAINS','tokenization_locale':'en'}");
  }
}
