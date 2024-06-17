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
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createMaterializedView;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import org.junit.Test;

public class CreateMaterializedViewTest {

  @Test
  public void should_not_throw_on_toString_for_CreateMaterializedViewStart() {
    assertThat(createMaterializedView("foo").toString()).isEqualTo("CREATE MATERIALIZED VIEW foo");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateMaterializedViewSelection() {
    assertThat(createMaterializedView("foo").asSelectFrom("bar").toString())
        .isEqualTo("CREATE MATERIALIZED VIEW foo");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateMaterializedViewWhereStart() {
    assertThat(createMaterializedView("foo").asSelectFrom("bar").all().toString())
        .isEqualTo("CREATE MATERIALIZED VIEW foo AS SELECT * FROM bar");
  }

  @Test
  public void should_generate_create_view_if_not_exists_with_select_all() {
    assertThat(
            createMaterializedView("baz")
                .ifNotExists()
                .asSelectFrom("foo", "bar")
                .all()
                .whereColumn("x")
                .isNotNull()
                .withPartitionKey("x"))
        .hasCql(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS baz AS SELECT * FROM foo.bar WHERE x IS NOT NULL PRIMARY KEY(x)");
  }

  @Test
  public void should_generate_create_view_with_select_columns() {
    assertThat(
            createMaterializedView("baz")
                .asSelectFrom("bar")
                .columns("x", "y")
                .whereColumn("x")
                .isNotNull()
                .whereColumn("y")
                .isLessThan(literal(5))
                .withPartitionKey("x"))
        .hasCql(
            "CREATE MATERIALIZED VIEW baz AS SELECT x,y FROM bar WHERE x IS NOT NULL AND y<5 PRIMARY KEY(x)");
  }

  @Test
  public void should_generate_create_view_with_compound_partition_key_and_clustering_columns() {
    assertThat(
            createMaterializedView("baz")
                .asSelectFrom("bar")
                .all()
                .whereColumn("x")
                .isNotNull()
                .whereColumn("y")
                .isNotNull()
                .withPartitionKey("x")
                .withPartitionKey("y")
                .withClusteringColumn("a")
                .withClusteringColumn("b"))
        .hasCql(
            "CREATE MATERIALIZED VIEW baz AS SELECT * FROM bar WHERE x IS NOT NULL AND y IS NOT NULL PRIMARY KEY((x,y),a,b)");
  }

  @Test
  public void should_generate_create_view_with_clustering_single() {
    assertThat(
            createMaterializedView("baz")
                .asSelectFrom("bar")
                .all()
                .whereColumn("x")
                .isNotNull()
                .withPartitionKey("x")
                .withClusteringColumn("a")
                .withClusteringOrder("a", ClusteringOrder.DESC))
        .hasCql(
            "CREATE MATERIALIZED VIEW baz AS SELECT * FROM bar WHERE x IS NOT NULL PRIMARY KEY(x,a) WITH CLUSTERING ORDER BY (a DESC)");
  }

  @Test
  public void should_generate_create_view_with_clustering_and_options() {
    assertThat(
            createMaterializedView("baz")
                .asSelectFrom("bar")
                .all()
                .whereColumn("x")
                .isNotNull()
                .withPartitionKey("x")
                .withClusteringColumn("a")
                .withClusteringColumn("b")
                .withClusteringOrder("a", ClusteringOrder.DESC)
                .withClusteringOrder("b", ClusteringOrder.ASC)
                .withCDC(true)
                .withComment("Hello"))
        .hasCql(
            "CREATE MATERIALIZED VIEW baz AS SELECT * FROM bar WHERE x IS NOT NULL PRIMARY KEY(x,a,b) WITH CLUSTERING ORDER BY (a DESC,b ASC) AND cdc=true AND comment='Hello'");
  }

  @Test
  public void should_generate_create_view_with_options() {
    assertThat(
            createMaterializedView("baz")
                .asSelectFrom("bar")
                .all()
                .whereColumn("x")
                .isNotNull()
                .withPartitionKey("x")
                .withCDC(true)
                .withComment("Hello"))
        .hasCql(
            "CREATE MATERIALIZED VIEW baz AS SELECT * FROM bar WHERE x IS NOT NULL PRIMARY KEY(x) WITH cdc=true AND comment='Hello'");
  }
}
