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
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createFunction;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.udt;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Test;

public class CreateFunctionTest {

  @Test
  public void should_not_throw_on_toString_for_CreateFunctionStart() {
    assertThat(createFunction("func1").toString())
        .isEqualTo("CREATE FUNCTION func1 () CALLED ON NULL INPUT");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateFunctionWithType() {
    assertThat(
            createFunction("func1")
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.INT)
                .toString())
        .isEqualTo("CREATE FUNCTION func1 (param1 int) RETURNS NULL ON NULL INPUT RETURNS int");
  }

  @Test
  public void should_not_throw_on_toString_for_CreateFunctionWithLanguage() {
    assertThat(
            createFunction("func1")
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
            createFunction("keyspace1", "func1")
                .withParameter("param1", DataTypes.INT)
                .calledOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return Integer.toString(param1);"))
        .hasCql(
            "CREATE FUNCTION keyspace1.func1 (param1 int) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_create_function_with_param_and_return_type_not_frozen() {
    assertThat(
            createFunction("keyspace1", "func6")
                .withParameter("param1", DataTypes.tupleOf(DataTypes.INT, DataTypes.INT))
                .returnsNullOnNull()
                .returnsType(udt("person", true))
                .withJavaLanguage()
                .as("'return Integer.toString(param1);'"))
        .hasCql(
            "CREATE FUNCTION keyspace1.func6 (param1 tuple<int, int>) RETURNS NULL ON NULL INPUT RETURNS person LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_honor_returns_null() {
    assertThat(
            createFunction("keyspace1", "func2")
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return Integer.toString(param1);"))
        .hasCql(
            "CREATE FUNCTION keyspace1.func2 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_create_function_with_many_params() {
    assertThat(
            createFunction("keyspace1", "func3")
                .withParameter("param1", DataTypes.INT)
                .withParameter("param2", DataTypes.TEXT)
                .withParameter("param3", DataTypes.BOOLEAN)
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return Integer.toString(param1);"))
        .hasCql(
            "CREATE FUNCTION keyspace1.func3 (param1 int,param2 text,param3 boolean) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_create_function_with_no_params() {

    assertThat(
            createFunction("keyspace1", "func4")
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withLanguage("java")
                .asQuoted("return \"hello world\";"))
        .hasCql(
            "CREATE FUNCTION keyspace1.func4 () RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return \"hello world\";'");
  }

  @Test
  public void should_create_function_with_no_keyspace() {
    assertThat(
            createFunction("func5")
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return \"hello world\";"))
        .hasCql(
            "CREATE FUNCTION func5 () RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return \"hello world\";'");
  }

  @Test
  public void should_create_function_with_if_not_exists() {
    assertThat(
            createFunction("keyspace1", "func6")
                .ifNotExists()
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return \"hello world\";"))
        .hasCql(
            "CREATE FUNCTION IF NOT EXISTS keyspace1.func6 () RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return \"hello world\";'");
  }

  @Test
  public void should_create_or_replace() {
    assertThat(
            createFunction("keyspace1", "func6")
                .orReplace()
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .asQuoted("return Integer.toString(param1);"))
        .hasCql(
            "CREATE OR REPLACE FUNCTION keyspace1.func6 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_not_quote_body_using_as() {
    assertThat(
            createFunction("keyspace1", "func6")
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaLanguage()
                .as("'return Integer.toString(param1);'"))
        .hasCql(
            "CREATE FUNCTION keyspace1.func6 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(param1);'");
  }

  @Test
  public void should_quote_with_dollar_signs_on_asQuoted_if_body_contains_single_quote() {
    assertThat(
            createFunction("keyspace1", "func6")
                .withParameter("param1", DataTypes.INT)
                .returnsNullOnNull()
                .returnsType(DataTypes.TEXT)
                .withJavaScriptLanguage()
                .asQuoted("'hello ' + param1;"))
        .hasCql(
            "CREATE FUNCTION keyspace1.func6 (param1 int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE javascript AS $$ 'hello ' + param1; $$");
  }
}
