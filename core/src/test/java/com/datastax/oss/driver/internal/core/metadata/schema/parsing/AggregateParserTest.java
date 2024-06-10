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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.util.Collections;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class AggregateParserTest extends SchemaParserTestBase {

  private static final AdminRow SUM_AND_TO_STRING_ROW_2_2 =
      mockAggregateRow(
          "ks",
          "sum_and_to_string",
          ImmutableList.of("org.apache.cassandra.db.marshal.Int32Type"),
          "plus",
          "org.apache.cassandra.db.marshal.Int32Type",
          "to_string",
          "org.apache.cassandra.db.marshal.UTF8Type",
          Bytes.fromHexString("0x00000000"));

  static final AdminRow SUM_AND_TO_STRING_ROW_3_0 =
      mockAggregateRow(
          "ks",
          "sum_and_to_string",
          ImmutableList.of("int"),
          "plus",
          "int",
          "to_string",
          "text",
          "0");

  @Before
  @Override
  public void setup() {
    super.setup();
    when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(context.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
  }

  @Test
  public void should_parse_modern_table() {
    AggregateParser parser = new AggregateParser(new DataTypeCqlNameParser(), context);
    AggregateMetadata aggregate =
        parser.parseAggregate(SUM_AND_TO_STRING_ROW_3_0, KEYSPACE_ID, Collections.emptyMap());

    assertThat(aggregate.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(aggregate.getSignature().getName().asInternal()).isEqualTo("sum_and_to_string");
    assertThat(aggregate.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);

    FunctionSignature stateFuncSignature = aggregate.getStateFuncSignature();
    assertThat(stateFuncSignature.getName().asInternal()).isEqualTo("plus");
    assertThat(stateFuncSignature.getParameterTypes())
        .containsExactly(DataTypes.INT, DataTypes.INT);
    assertThat(aggregate.getStateType()).isEqualTo(DataTypes.INT);

    Optional<FunctionSignature> finalFuncSignature = aggregate.getFinalFuncSignature();
    assertThat(finalFuncSignature).isPresent();
    assertThat(finalFuncSignature)
        .hasValueSatisfying(
            signature -> {
              assertThat(signature.getName().asInternal()).isEqualTo("to_string");
              assertThat(signature.getParameterTypes()).containsExactly(DataTypes.INT);
            });
    assertThat(aggregate.getReturnType()).isEqualTo(DataTypes.TEXT);

    assertThat(aggregate.getInitCond().get()).isInstanceOf(Integer.class).isEqualTo(0);
  }

  @Test
  public void should_parse_legacy_table() {
    AggregateParser parser = new AggregateParser(new DataTypeClassNameParser(), context);
    AggregateMetadata aggregate =
        parser.parseAggregate(SUM_AND_TO_STRING_ROW_2_2, KEYSPACE_ID, Collections.emptyMap());

    assertThat(aggregate.getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(aggregate.getSignature().getName().asInternal()).isEqualTo("sum_and_to_string");
    assertThat(aggregate.getSignature().getParameterTypes()).containsExactly(DataTypes.INT);

    FunctionSignature stateFuncSignature = aggregate.getStateFuncSignature();
    assertThat(stateFuncSignature.getName().asInternal()).isEqualTo("plus");
    assertThat(stateFuncSignature.getParameterTypes())
        .containsExactly(DataTypes.INT, DataTypes.INT);
    assertThat(aggregate.getStateType()).isEqualTo(DataTypes.INT);

    Optional<FunctionSignature> finalFuncSignature = aggregate.getFinalFuncSignature();
    assertThat(finalFuncSignature).isPresent();
    assertThat(finalFuncSignature)
        .hasValueSatisfying(
            signature -> {
              assertThat(signature.getName().asInternal()).isEqualTo("to_string");
              assertThat(signature.getParameterTypes()).containsExactly(DataTypes.INT);
            });
    assertThat(aggregate.getReturnType()).isEqualTo(DataTypes.TEXT);

    assertThat(aggregate.getInitCond().get()).isInstanceOf(Integer.class).isEqualTo(0);
  }
}
