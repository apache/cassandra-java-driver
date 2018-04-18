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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Collections;
import org.junit.Test;

public class FunctionParserTest extends SchemaParserTestBase {

  private static final AdminRow ID_ROW_2_2 =
      mockFunctionRow(
          "ks",
          "id",
          ImmutableList.of("i"),
          ImmutableList.of("org.apache.cassandra.db.marshal.Int32Type"),
          "return i;",
          false,
          "java",
          "org.apache.cassandra.db.marshal.Int32Type");

  static final AdminRow ID_ROW_3_0 =
      mockFunctionRow(
          "ks",
          "id",
          ImmutableList.of("i"),
          ImmutableList.of("int"),
          "return i;",
          false,
          "java",
          "int");

  @Test
  public void should_parse_modern_table() {
    FunctionParser parser = new FunctionParser(new DataTypeCqlNameParser(), context);
    FunctionMetadata function =
        parser.parseFunction(ID_ROW_3_0, KEYSPACE_ID, Collections.emptyMap());

    assertThat(function.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(function.getSignature().getName().asInternal()).isEqualTo("id");
    assertThat(function.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);
    assertThat(function.getParameterNames()).containsExactly(CqlIdentifier.fromInternal("i"));
    assertThat(function.getBody()).isEqualTo("return i;");
    assertThat(function.isCalledOnNullInput()).isFalse();
    assertThat(function.getLanguage()).isEqualTo("java");
    assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
  }

  @Test
  public void should_parse_legacy_table() {
    FunctionParser parser = new FunctionParser(new DataTypeClassNameParser(), context);
    FunctionMetadata function =
        parser.parseFunction(ID_ROW_2_2, KEYSPACE_ID, Collections.emptyMap());

    assertThat(function.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(function.getSignature().getName().asInternal()).isEqualTo("id");
    assertThat(function.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);
    assertThat(function.getParameterNames()).containsExactly(CqlIdentifier.fromInternal("i"));
    assertThat(function.getBody()).isEqualTo("return i;");
    assertThat(function.isCalledOnNullInput()).isFalse();
    assertThat(function.getLanguage()).isEqualTo("java");
    assertThat(function.getReturnType()).isEqualTo(DataTypes.INT);
  }
}
