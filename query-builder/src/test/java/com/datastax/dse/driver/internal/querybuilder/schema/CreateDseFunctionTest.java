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
package com.datastax.dse.driver.internal.querybuilder.schema;

import static com.datastax.dse.driver.api.querybuilder.DseSchemaBuilder.createDseFunction;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.udt;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Test;

/**
 * Tests for creating DSE extended functions. Most of these tests are copied from the OSS {@code
 * com.datastax.oss.driver.internal.querybuilder.schema.CreateFunctionTest} class to ensure DSE
 * extended behavior does not break OSS functionality, with additional tests to verify the DSE
 * specific functionality (i.e. the DETERMINISTIC and MONOTONIC keywords).
 */
public class CreateDseFunctionTest {

  @Test
  public void should_not_throw_on_toString_for_CreateFunctionStart() {
    String funcStr = createDseFunction("func1").toString();
    assertThat(funcStr).isEqualTo("CREATE FUNCTION func1 () CALLED ON NULL INPUT");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateFunctionWithType() {
    assertThat(
            createDseFunction("func1")
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.INT)
                .toString())
        .isEqualTo("CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS int");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateFunctionWithLanguage() {
    assertThat(
            createDseFunction("func1")
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.INT)
                .withJavaLanguage()
                .toString())
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java");
  }

  @Test
  public void should_create_function_with_simple_params() {
    assertThat(
            createDseFunction("keyspace1", "func1")
                .withParameter("param1", DataTypes.INT)
                .calledOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return Integer.toString(param1);")
                .asCql())
        .isEqualTo(
            "CREATE FUNCTION keyspace1.func1 (param1 int) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_create_function_with_param_and_return_type_not_frozen() {
    assertThat(
            createDseFunction("keyspace1", "func6")
                .withParameter("param1", DataTypes.tupleOf(DataTypes.INT, DataTypes.INT))
                .returnsNullOnNull()
                .returnsType(udt("person", true))
                .withJavaLanguage()
                .as("'return Integer.toString(param1);'")
                .asCql())
        .isEqualTo(
            "CREATE FUNCTION keyspace1.func6 (param1 tuple<int, int>) RETURNS NULL ON NULL INPUT RETURNS person LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_honor_returns_null() {
    assertThat(
            createDseFunction("keyspace1", "func2")
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return Integer.toString(param1);")
                .asCql())
        .isEqualTo(
            "CREATE FUNCTION keyspace1.func2 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_create_function_with_many_params() {
    assertThat(
            createDseFunction("keyspace1", "func3")
                .withParameter("param1", DataTypes.INT)
                .withParameter("param2", DataTypes.TEXT)
                .withParameter("param3", DataTypes.BOOLEAN)
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return Integer.toString(param1);")
                .asCql())
        .isEqualTo(
            "CREATE FUNCTION keyspace1.func3 (param1 int,param2 text,param3 boolean) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_create_function_with_no_params() {

    assertThat(
            createDseFunction("keyspace1", "func4")
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withLanguage("java")
                .asQuoted("return \"hello world\";")
                .asCql())
        .isEqualTo(
            "CREATE FUNCTION keyspace1.func4 () RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return \"hello world\";'");
  }

  @Test
  public void should_create_function_with_no_keyspace() {
    assertThat(
            createDseFunction("func5")
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return \"hello world\";")
                .asCql())
        .isEqualTo(
            "CREATE FUNCTION func5 () RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return \"hello world\";'");
  }

  @Test
  public void should_create_function_with_if_not_exists() {
    assertThat(
            createDseFunction("keyspace1", "func6")
                .ifNotExists()
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return \"hello world\";")
                .asCql())
        .isEqualTo(
            "CREATE FUNCTION IF NOT EXISTS keyspace1.func6 () RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return \"hello world\";'");
  }

  @Test
  public void should_create_or_replace() {
    assertThat(
            createDseFunction("keyspace1", "func6")
                .orReplace()
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return Integer.toString(param1);")
                .asCql())
        .isEqualTo(
            "CREATE OR REPLACE FUNCTION keyspace1.func6 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_not_quote_body_using_as() {
    assertThat(
            createDseFunction("keyspace1", "func6")
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .as("'return Integer.toString(param1);'")
                .asCql())
        .isEqualTo(
            "CREATE FUNCTION keyspace1.func6 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_quote_with_dollar_signs_on_asQuoted_if_body_contains_single_quote() {
    assertThat(
            createDseFunction("keyspace1", "func6")
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaScriptLanguage()
                .asQuoted("'hello ' + param1;")
                .asCql())
        .isEqualTo(
            "CREATE FUNCTION keyspace1.func6 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE javascript AS $$ 'hello ' + param1; $$");
  }

  @Test
  public void should_not_throw_on_toString_for_create_function_with_deterministic() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.INT)
            .deterministic()
            .toString();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS int DETERMINISTIC");
  }

  @Test
  public void should_not_quote_body_using_as_with_deterministic() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.TEXT)
            .deterministic()
            .withJavaLanguage()
            .as("'return Integer.toString(param1);'")
            .asCql();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text DETERMINISTIC LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void
      should_quote_with_dollar_signs_on_asQuoted_if_body_contains_single_quote_with_deterministic() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.TEXT)
            .deterministic()
            .withJavaScriptLanguage()
            .asQuoted("'hello ' + param1;")
            .asCql();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text DETERMINISTIC LANGUAGE javascript AS $$ 'hello ' + param1; $$");
  }

  @Test
  public void should_not_throw_on_toString_for_create_function_with_monotonic() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.INT)
            .monotonic()
            .toString();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS int MONOTONIC");
  }

  @Test
  public void should_not_quote_body_using_as_with_monotonic() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.TEXT)
            .monotonic()
            .withJavaLanguage()
            .as("'return Integer.toString(param1);'")
            .asCql();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text MONOTONIC LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void
      should_quote_with_dollar_signs_on_asQuoted_if_body_contains_single_quote_with_monotonic() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.TEXT)
            .monotonic()
            .withJavaScriptLanguage()
            .asQuoted("'hello ' + param1;")
            .asCql();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text MONOTONIC LANGUAGE javascript AS $$ 'hello ' + param1; $$");
  }

  @Test
  public void should_not_throw_on_toString_for_create_function_with_monotonic_on() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .withParameter("param2", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.INT)
            .monotonicOn("param2")
            .toString();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int,param2 int) RETURNS NULL ON NULL INPUT RETURNS int MONOTONIC ON param2");
  }

  @Test
  public void should_not_quote_body_using_as_with_monotonic_on() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .withParameter("param2", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.TEXT)
            .monotonicOn("param2")
            .withJavaLanguage()
            .as("'return Integer.toString(param1);'")
            .asCql();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int,param2 int) RETURNS NULL ON NULL INPUT RETURNS text MONOTONIC ON param2 LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void
      should_quote_with_dollar_signs_on_asQuoted_if_body_contains_single_quote_with_monotonic_on() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .withParameter("param2", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.TEXT)
            .monotonicOn("param2")
            .withJavaScriptLanguage()
            .asQuoted("'hello ' + param1;")
            .asCql();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int,param2 int) RETURNS NULL ON NULL INPUT RETURNS text MONOTONIC ON param2 LANGUAGE javascript AS $$ 'hello ' + param1; $$");
  }

  @Test
  public void should_not_throw_on_toString_for_create_function_with_deterministic_and_monotonic() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.INT)
            .deterministic()
            .monotonic()
            .toString();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS int DETERMINISTIC MONOTONIC");
  }

  @Test
  public void should_not_quote_body_using_as_with_deterministic_and_monotonic() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.TEXT)
            .deterministic()
            .monotonic()
            .withJavaLanguage()
            .as("'return Integer.toString(param1);'")
            .asCql();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text DETERMINISTIC MONOTONIC LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void
      should_quote_with_dollar_signs_on_asQuoted_if_body_contains_single_quote_with_deterministic_and_monotonic() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.TEXT)
            .deterministic()
            .monotonic()
            .withJavaScriptLanguage()
            .asQuoted("'hello ' + param1;")
            .asCql();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text DETERMINISTIC MONOTONIC LANGUAGE javascript AS $$ 'hello ' + param1; $$");
  }

  @Test
  public void
      should_not_throw_on_toString_for_create_function_with_deterministic_and_monotonic_on() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .withParameter("param2", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.INT)
            .deterministic()
            .monotonicOn("param2")
            .toString();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int,param2 int) RETURNS NULL ON NULL INPUT RETURNS int DETERMINISTIC MONOTONIC ON param2");
  }

  @Test
  public void should_not_quote_body_using_as_with_deterministic_and_monotonic_on() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .withParameter("param2", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.TEXT)
            .deterministic()
            .monotonicOn("param2")
            .withJavaLanguage()
            .as("'return Integer.toString(param1);'")
            .asCql();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int,param2 int) RETURNS NULL ON NULL INPUT RETURNS text DETERMINISTIC MONOTONIC ON param2 LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void
      should_quote_with_dollar_signs_on_asQuoted_if_body_contains_single_quote_with_deterministic_and_monotonic_on() {
    final String funcStr =
        createDseFunction("func1")
            .withParameter("param1", DataTypes.INT)
            .withParameter("param2", DataTypes.INT)
            .returnsNullOnNull()
            .returnsType(DataTypes.TEXT)
            .deterministic()
            .monotonicOn("param2")
            .withJavaScriptLanguage()
            .asQuoted("'hello ' + param1;")
            .asCql();
    assertThat(funcStr)
        .isEqualTo(
            "CREATE FUNCTION func1 (param1 int,param2 int) RETURNS NULL ON NULL INPUT RETURNS text DETERMINISTIC MONOTONIC ON param2 LANGUAGE javascript AS $$ 'hello ' + param1; $$");
  }
}
