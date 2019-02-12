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
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createAggregate;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Test;

public class CreateAggregateTest {
  @Test
  public void should_create_aggreate_with_simple_param() {

    assertThat(
            createAggregate("keyspace1", "agg1")
                .withParameter(DataTypes.INT)
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0))))
        .hasCql(
            "CREATE AGGREGATE keyspace1.agg1 (int) SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_many_params() {

    assertThat(
            createAggregate("keyspace1", "agg2")
                .withParameter(DataTypes.INT)
                .withParameter(DataTypes.TEXT)
                .withParameter(DataTypes.BOOLEAN)
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0))))
        .hasCql(
            "CREATE AGGREGATE keyspace1.agg2 (int,text,boolean) SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_param_without_frozen() {

    assertThat(
            createAggregate("keyspace1", "agg9")
                .withParameter(DataTypes.tupleOf(DataTypes.TEXT))
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0))))
        .hasCql(
            "CREATE AGGREGATE keyspace1.agg9 (tuple<text>) SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_no_params() {

    assertThat(
            createAggregate("keyspace1", "agg3")
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0))))
        .hasCql(
            "CREATE AGGREGATE keyspace1.agg3 () SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_no_keyspace() {

    assertThat(
            createAggregate("agg4")
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0))))
        .hasCql(
            "CREATE AGGREGATE agg4 () SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_if_not_exists() {

    assertThat(
            createAggregate("agg6")
                .ifNotExists()
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0))))
        .hasCql(
            "CREATE AGGREGATE IF NOT EXISTS agg6 () SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_create_aggregate_with_no_final_func() {

    assertThat(
            createAggregate("cycling", "sum")
                .withParameter(DataTypes.INT)
                .withSFunc("dsum")
                .withSType(DataTypes.INT))
        .hasCql("CREATE AGGREGATE cycling.sum (int) SFUNC dsum STYPE int");
  }

  @Test
  public void should_create_or_replace() {
    assertThat(
            createAggregate("keyspace1", "agg7")
                .orReplace()
                .withSFunc("sfunction")
                .withSType(DataTypes.ASCII)
                .withFinalFunc("finalfunction")
                .withInitCond(tuple(literal(0), literal(0))))
        .hasCql(
            "CREATE OR REPLACE AGGREGATE keyspace1.agg7 () SFUNC sfunction STYPE ascii FINALFUNC finalfunction INITCOND (0,0)");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateAggregateStart() {
    assertThat(createAggregate("agg1").toString()).isEqualTo("CREATE AGGREGATE agg1 ()");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateAggregateWithParam() {
    assertThat(createAggregate("func1").withParameter(DataTypes.INT).toString())
        .isEqualTo("CREATE AGGREGATE func1 (int)");
  }

  @Test
  public void should_not_throw_on_toString_for_NotExists_OrReplace() {
    assertThat(createAggregate("func1").ifNotExists().orReplace().toString())
        .isEqualTo("CREATE OR REPLACE AGGREGATE IF NOT EXISTS func1 ()");
  }
}
