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
package com.datastax.dse.driver.internal.querybuilder.schema;

import static com.datastax.dse.driver.api.querybuilder.DseSchemaBuilder.createDseAggregate;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Test;

/**
 * Tests for creating DSE extended aggregates. Most of these tests are copied from the OSS {@code
 * com.datastax.oss.driver.internal.querybuilder.schema.CreateAggregateTest} class to ensure DSE
 * extended behavior does not break OSS functionality, with additional tests to verify the DSE
 * specific functionality (i.e. the DETERMINISTIC keyword).
 */
public class CreateDseAggregateTest {

  @Test
  public void should_create_aggreate_with_simple_param() {

    assertThat(
            createDseAggregate("keyspace1", "agg1")
                .withParameter(DataTypes.INT)
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0)))
                .asCql())
        .isEqualTo(
            "CREATE AGGREGATE keyspace1.agg1 (int) SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_many_params() {

    assertThat(
            createDseAggregate("keyspace1", "agg2")
                .withParameter(DataTypes.INT)
                .withParameter(DataTypes.TEXT)
                .withParameter(DataTypes.BOOLEAN)
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0)))
                .asCql())
        .isEqualTo(
            "CREATE AGGREGATE keyspace1.agg2 (int,text,boolean) SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_param_without_frozen() {

    assertThat(
            createDseAggregate("keyspace1", "agg9")
                .withParameter(DataTypes.tupleOf(DataTypes.TEXT))
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0)))
                .asCql())
        .isEqualTo(
            "CREATE AGGREGATE keyspace1.agg9 (tuple<text>) SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_no_params() {

    assertThat(
            createDseAggregate("keyspace1", "agg3")
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0)))
                .asCql())
        .isEqualTo(
            "CREATE AGGREGATE keyspace1.agg3 () SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_no_keyspace() {

    assertThat(
            createDseAggregate("agg4")
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0)))
                .asCql())
        .isEqualTo(
            "CREATE AGGREGATE agg4 () SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_if_not_exists() {

    assertThat(
            createDseAggregate("agg6")
                .ifNotExists()
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0)))
                .asCql())
        .isEqualTo(
            "CREATE AGGREGATE IF NOT EXISTS agg6 () SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_no_final_func() {

    assertThat(
            createDseAggregate("cycling", "sum")
                .withParameter(DataTypes.INT)
                .withSFunc("dsum")
                .withSType(DataTypes.INT)
                .asCql())
        .isEqualTo("CREATE AGGREGATE cycling.sum (int) SFUNC dsum STYPE int");
  }

  @Test
  public void should_create_or_replace() {
    assertThat(
            createDseAggregate("keyspace1", "agg7")
                .orReplace()
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0)))
                .asCql())
        .isEqualTo(
            "CREATE OR REPLACE AGGREGATE keyspace1.agg7 () SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateAggregateStart() {
    assertThat(createDseAggregate("agg1").toString()).isEqualTo("CREATE AGGREGATE agg1 ()");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateAggregateWithParam() {
    assertThat(createDseAggregate("func1").withParameter(DataTypes.INT).toString())
        .isEqualTo("CREATE AGGREGATE func1 (int)");
  }

  @Test
  public void should_not_throw_on_toString_for_NotExists_OrReplace() {
    assertThat(createDseAggregate("func1").ifNotExists().orReplace().toString())
        .isEqualTo("CREATE OR REPLACE AGGREGATE IF NOT EXISTS func1 ()");
  }

  @Test
  public void should_create_aggregate_with_deterministic() {

    assertThat(
            createDseAggregate("keyspace1", "agg1")
                .withParameter(DataTypes.INT)
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0)))
                .deterministic()
                .asCql())
        .isEqualTo(
            "CREATE AGGREGATE keyspace1.agg1 (int) SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0) DETERMINISTIC");
  }
}
