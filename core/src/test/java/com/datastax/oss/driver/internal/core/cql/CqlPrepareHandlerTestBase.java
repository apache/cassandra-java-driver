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
package com.datastax.oss.driver.internal.core.cql;

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public abstract class CqlPrepareHandlerTestBase {

  protected static final DefaultPrepareRequest PREPARE_REQUEST =
      new DefaultPrepareRequest("mock query");

  @Mock protected Node node1;
  @Mock protected Node node2;
  @Mock protected Node node3;

  protected final Map<String, ByteBuffer> payload =
      ImmutableMap.of("key1", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  protected static Message simplePrepared() {
    RowsMetadata variablesMetadata =
        new RowsMetadata(
            ImmutableList.of(
                new ColumnSpec(
                    "ks",
                    "table",
                    "key",
                    0,
                    RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR))),
            null,
            new int[] {0},
            null);
    RowsMetadata resultMetadata =
        new RowsMetadata(
            ImmutableList.of(
                new ColumnSpec(
                    "ks",
                    "table",
                    "message",
                    0,
                    RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR))),
            null,
            new int[] {},
            null);
    return new Prepared(
        Bytes.fromHexString("0xffff").array(), null, variablesMetadata, resultMetadata);
  }

  protected static void assertMatchesSimplePrepared(PreparedStatement statement) {
    assertThat(Bytes.toHexString(statement.getId())).isEqualTo("0xffff");

    ColumnDefinitions variableDefinitions = statement.getVariableDefinitions();
    assertThat(variableDefinitions).hasSize(1);
    assertThat(variableDefinitions.get(0).getName().asInternal()).isEqualTo("key");

    ColumnDefinitions resultSetDefinitions = statement.getResultSetDefinitions();
    assertThat(resultSetDefinitions).hasSize(1);
    assertThat(resultSetDefinitions.get(0).getName().asInternal()).isEqualTo("message");
  }
}
