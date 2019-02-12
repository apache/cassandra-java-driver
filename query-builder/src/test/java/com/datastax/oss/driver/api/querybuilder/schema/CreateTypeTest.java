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
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createType;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.udt;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Test;

public class CreateTypeTest {

  @Test
  public void should_not_throw_on_toString_for_CreateTypeStart() {
    assertThat(createType("foo").toString()).isEqualTo("CREATE TYPE foo");
  }

  @Test
  public void should_create_type_with_single_field() {

    assertThat(createType("keyspace1", "type").withField("single", DataTypes.TEXT))
        .hasCql("CREATE TYPE keyspace1.type (single text)");
    assertThat(createType("type").withField("single", DataTypes.TEXT))
        .hasCql("CREATE TYPE type (single text)");

    assertThat(createType("type").ifNotExists().withField("single", DataTypes.TEXT))
        .hasCql("CREATE TYPE IF NOT EXISTS type (single text)");
  }

  @Test
  public void should_create_type_with_many_fields() {

    assertThat(
            createType("keyspace1", "type")
                .withField("first", DataTypes.TEXT)
                .withField("second", DataTypes.INT)
                .withField("third", DataTypes.BLOB)
                .withField("fourth", DataTypes.BOOLEAN))
        .hasCql("CREATE TYPE keyspace1.type (first text,second int,third blob,fourth boolean)");
    assertThat(
            createType("type")
                .withField("first", DataTypes.TEXT)
                .withField("second", DataTypes.INT)
                .withField("third", DataTypes.BLOB)
                .withField("fourth", DataTypes.BOOLEAN))
        .hasCql("CREATE TYPE type (first text,second int,third blob,fourth boolean)");
  }

  @Test
  public void should_create_type_with_nested_UDT() {
    assertThat(createType("keyspace1", "type").withField("nested", udt("val", true)))
        .hasCql("CREATE TYPE keyspace1.type (nested frozen<val>)");
    assertThat(createType("keyspace1", "type").withField("nested", udt("val", false)))
        .hasCql("CREATE TYPE keyspace1.type (nested val)");
  }

  @Test
  public void should_create_type_with_collections() {
    assertThat(createType("ks1", "type").withField("names", DataTypes.listOf(DataTypes.TEXT)))
        .hasCql("CREATE TYPE ks1.type (names list<text>)");

    assertThat(createType("ks1", "type").withField("names", DataTypes.tupleOf(DataTypes.TEXT)))
        .hasCql("CREATE TYPE ks1.type (names frozen<tuple<text>>)");

    assertThat(
            createType("ks1", "type")
                .withField("map", DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT)))
        .hasCql("CREATE TYPE ks1.type (map map<int, text>)");
  }
}
