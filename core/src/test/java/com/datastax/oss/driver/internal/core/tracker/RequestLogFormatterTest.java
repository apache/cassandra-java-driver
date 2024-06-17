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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.tracker;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.cql.DefaultPreparedStatement;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestLogFormatterTest {

  @Mock private DriverContext context;
  private final ProtocolVersion protocolVersion = DefaultProtocolVersion.V4;

  private RequestLogFormatter formatter;

  @Before
  public void setup() {
    when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(context.getProtocolVersion()).thenReturn(protocolVersion);

    formatter = new RequestLogFormatter(context);
  }

  @Test
  public void should_format_simple_statement_without_values() {
    SimpleStatement statement =
        SimpleStatement.newInstance("SELECT release_version FROM system.local");

    assertThat(
            formatRequest(
                statement, Integer.MAX_VALUE, false, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[0 values] SELECT release_version FROM system.local");

    assertThat(
            formatRequest(statement, Integer.MAX_VALUE, true, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[0 values] SELECT release_version FROM system.local");

    assertThat(formatRequest(statement, 20, false, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[0 values] SELECT release_versi...<truncated>");
  }

  @Test
  public void should_format_simple_statement_with_positional_values() {
    SimpleStatement statement =
        SimpleStatement.builder("UPDATE foo SET v=? WHERE k=?")
            .addPositionalValue(Bytes.fromHexString("0xdeadbeef"))
            .addPositionalValue(0)
            .build();

    assertThat(
            formatRequest(
                statement, Integer.MAX_VALUE, false, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[2 values] UPDATE foo SET v=? WHERE k=?");

    assertThat(
            formatRequest(statement, Integer.MAX_VALUE, true, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[2 values] UPDATE foo SET v=? WHERE k=? [v0=0xdeadbeef, v1=0]");

    assertThat(formatRequest(statement, Integer.MAX_VALUE, true, 1, Integer.MAX_VALUE))
        .isEqualTo(
            "[2 values] UPDATE foo SET v=? WHERE k=? [v0=0xdeadbeef, ...<further values truncated>]");

    assertThat(formatRequest(statement, Integer.MAX_VALUE, true, Integer.MAX_VALUE, 4))
        .isEqualTo("[2 values] UPDATE foo SET v=? WHERE k=? [v0=0xde...<truncated>, v1=0]");
  }

  @Test
  public void should_format_simple_statement_with_named_values() {
    SimpleStatement statement =
        SimpleStatement.builder("UPDATE foo SET v=:v WHERE k=:k")
            .addNamedValue("v", Bytes.fromHexString("0xdeadbeef"))
            .addNamedValue("k", 0)
            .build();

    assertThat(
            formatRequest(
                statement, Integer.MAX_VALUE, false, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[2 values] UPDATE foo SET v=:v WHERE k=:k");

    assertThat(
            formatRequest(statement, Integer.MAX_VALUE, true, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[2 values] UPDATE foo SET v=:v WHERE k=:k [v=0xdeadbeef, k=0]");

    assertThat(formatRequest(statement, Integer.MAX_VALUE, true, 1, Integer.MAX_VALUE))
        .isEqualTo(
            "[2 values] UPDATE foo SET v=:v WHERE k=:k [v=0xdeadbeef, ...<further values truncated>]");

    assertThat(formatRequest(statement, Integer.MAX_VALUE, true, Integer.MAX_VALUE, 4))
        .isEqualTo("[2 values] UPDATE foo SET v=:v WHERE k=:k [v=0xde...<truncated>, k=0]");
  }

  @Test
  public void should_format_bound_statement() {
    PreparedStatement preparedStatement =
        mockPreparedStatement(
            "UPDATE foo SET v=? WHERE k=?",
            ImmutableMap.of("v", DataTypes.BLOB, "k", DataTypes.INT));
    BoundStatement statement = preparedStatement.bind(Bytes.fromHexString("0xdeadbeef"), 0);

    assertThat(
            formatRequest(
                statement, Integer.MAX_VALUE, false, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[2 values] UPDATE foo SET v=? WHERE k=?");

    assertThat(
            formatRequest(statement, Integer.MAX_VALUE, true, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[2 values] UPDATE foo SET v=? WHERE k=? [v=0xdeadbeef, k=0]");

    assertThat(formatRequest(statement, Integer.MAX_VALUE, true, 1, Integer.MAX_VALUE))
        .isEqualTo(
            "[2 values] UPDATE foo SET v=? WHERE k=? [v=0xdeadbeef, ...<further values truncated>]");

    assertThat(formatRequest(statement, Integer.MAX_VALUE, true, Integer.MAX_VALUE, 4))
        .isEqualTo("[2 values] UPDATE foo SET v=? WHERE k=? [v=0xde...<truncated>, k=0]");
  }

  @Test
  public void should_format_bound_statement_with_unset_values() {
    PreparedStatement preparedStatement =
        mockPreparedStatement(
            "UPDATE foo SET v=? WHERE k=?",
            ImmutableMap.of("v", DataTypes.BLOB, "k", DataTypes.INT));
    BoundStatement statement = preparedStatement.bind().setInt("k", 0);
    assertThat(
            formatRequest(statement, Integer.MAX_VALUE, true, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[2 values] UPDATE foo SET v=? WHERE k=? [v=<UNSET>, k=0]");
  }

  @Test
  public void should_format_batch_statement() {
    SimpleStatement statement1 =
        SimpleStatement.builder("UPDATE foo SET v=? WHERE k=?")
            .addNamedValue("v", Bytes.fromHexString("0xdeadbeef"))
            .addNamedValue("k", 0)
            .build();

    PreparedStatement preparedStatement =
        mockPreparedStatement(
            "UPDATE foo SET v=? WHERE k=?",
            ImmutableMap.of("v", DataTypes.BLOB, "k", DataTypes.INT));
    BoundStatement statement2 = preparedStatement.bind(Bytes.fromHexString("0xabcdef"), 1);

    BatchStatement batch =
        BatchStatement.builder(DefaultBatchType.UNLOGGED)
            .addStatements(statement1, statement2)
            .build();

    assertThat(formatRequest(batch, Integer.MAX_VALUE, false, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo(
            "[2 statements, 4 values] "
                + "BEGIN UNLOGGED BATCH "
                + "UPDATE foo SET v=? WHERE k=?; "
                + "UPDATE foo SET v=? WHERE k=?; "
                + "APPLY BATCH");

    assertThat(formatRequest(batch, 20, false, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo("[2 statements, 4 values] BEGIN UNLOGGED BATCH...<truncated>");

    assertThat(formatRequest(batch, Integer.MAX_VALUE, true, Integer.MAX_VALUE, Integer.MAX_VALUE))
        .isEqualTo(
            "[2 statements, 4 values] "
                + "BEGIN UNLOGGED BATCH "
                + "UPDATE foo SET v=? WHERE k=?; "
                + "UPDATE foo SET v=? WHERE k=?; "
                + "APPLY BATCH "
                + "[v=0xdeadbeef, k=0]"
                + "[v=0xabcdef, k=1]");

    assertThat(formatRequest(batch, Integer.MAX_VALUE, true, 3, Integer.MAX_VALUE))
        .isEqualTo(
            "[2 statements, 4 values] "
                + "BEGIN UNLOGGED BATCH "
                + "UPDATE foo SET v=? WHERE k=?; "
                + "UPDATE foo SET v=? WHERE k=?; "
                + "APPLY BATCH "
                + "[v=0xdeadbeef, k=0]"
                + "[v=0xabcdef, ...<further values truncated>]");

    assertThat(formatRequest(batch, Integer.MAX_VALUE, true, 2, Integer.MAX_VALUE))
        .isEqualTo(
            "[2 statements, 4 values] "
                + "BEGIN UNLOGGED BATCH "
                + "UPDATE foo SET v=? WHERE k=?; "
                + "UPDATE foo SET v=? WHERE k=?; "
                + "APPLY BATCH "
                + "[v=0xdeadbeef, k=0]"
                + "[...<further values truncated>]");

    assertThat(formatRequest(batch, Integer.MAX_VALUE, true, Integer.MAX_VALUE, 4))
        .isEqualTo(
            "[2 statements, 4 values] "
                + "BEGIN UNLOGGED BATCH "
                + "UPDATE foo SET v=? WHERE k=?; "
                + "UPDATE foo SET v=? WHERE k=?; "
                + "APPLY BATCH "
                + "[v=0xde...<truncated>, k=0]"
                + "[v=0xab...<truncated>, k=1]");
  }

  private String formatRequest(
      Request request, int maxQueryLength, boolean showValues, int maxValues, int maxValueLength) {
    StringBuilder builder = new StringBuilder();
    formatter.appendRequest(
        request, maxQueryLength, showValues, maxValues, maxValueLength, builder);
    return builder.toString();
  }

  private PreparedStatement mockPreparedStatement(String query, Map<String, DataType> variables) {
    ImmutableList.Builder<ColumnDefinition> definitions = ImmutableList.builder();
    int i = 0;
    for (Map.Entry<String, DataType> entry : variables.entrySet()) {
      definitions.add(
          new DefaultColumnDefinition(
              new ColumnSpec(
                  "test",
                  "foo",
                  entry.getKey(),
                  i,
                  RawType.PRIMITIVES.get(entry.getValue().getProtocolCode())),
              context));
    }
    return new DefaultPreparedStatement(
        Bytes.fromHexString("0x"),
        query,
        DefaultColumnDefinitions.valueOf(definitions.build()),
        Collections.emptyList(),
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        null,
        null,
        Integer.MIN_VALUE,
        null,
        null,
        false,
        context.getCodecRegistry(),
        context.getProtocolVersion(),
        false);
  }
}
