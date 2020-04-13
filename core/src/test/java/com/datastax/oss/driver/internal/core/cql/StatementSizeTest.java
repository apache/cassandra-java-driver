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
package com.datastax.oss.driver.internal.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.DefaultProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StatementSizeTest {

  private static final byte[] MOCK_PAGING_STATE = Bytes.getArray(Bytes.fromHexString("0xdeadbeef"));
  private static final ByteBuffer MOCK_PAYLOAD_VALUE1 = Bytes.fromHexString("0xabcd");
  private static final ByteBuffer MOCK_PAYLOAD_VALUE2 = Bytes.fromHexString("0xef");
  private static final ImmutableMap<String, ByteBuffer> MOCK_PAYLOAD =
      ImmutableMap.of("key1", MOCK_PAYLOAD_VALUE1, "key2", MOCK_PAYLOAD_VALUE2);
  private static final byte[] PREPARED_ID = Bytes.getArray(Bytes.fromHexString("0xaaaa"));
  private static final byte[] RESULT_METADATA_ID = Bytes.getArray(Bytes.fromHexString("0xbbbb"));

  @Mock PreparedStatement preparedStatement;
  @Mock InternalDriverContext driverContext;
  @Mock DriverConfig config;
  @Mock DriverExecutionProfile defaultProfile;
  @Mock TimestampGenerator timestampGenerator;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    ByteBuffer preparedId = ByteBuffer.wrap(PREPARED_ID);
    when(preparedStatement.getId()).thenReturn(preparedId);
    ByteBuffer resultMetadataId = ByteBuffer.wrap(RESULT_METADATA_ID);
    when(preparedStatement.getResultMetadataId()).thenReturn(resultMetadataId);

    ColumnDefinitions columnDefinitions =
        DefaultColumnDefinitions.valueOf(
            ImmutableList.of(
                phonyColumnDef("ks", "table", "c1", -1, ProtocolConstants.DataType.INT),
                phonyColumnDef("ks", "table", "c2", -1, ProtocolConstants.DataType.VARCHAR)));

    when(preparedStatement.getVariableDefinitions()).thenReturn(columnDefinitions);

    when(driverContext.getProtocolVersion()).thenReturn(DefaultProtocolVersion.V5);
    when(driverContext.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(driverContext.getProtocolVersionRegistry())
        .thenReturn(new DefaultProtocolVersionRegistry(null));
    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(driverContext.getConfig()).thenReturn(config);
    when(driverContext.getTimestampGenerator()).thenReturn(timestampGenerator);
  }

  private ColumnDefinition phonyColumnDef(
      String keyspace, String table, String column, int index, int typeCode) {
    return new DefaultColumnDefinition(
        new ColumnSpec(keyspace, table, column, index, RawType.PRIMITIVES.get(typeCode)),
        AttachmentPoint.NONE);
  }

  @Test
  public void should_measure_size_of_simple_statement() {
    String queryString = "SELECT release_version FROM system.local WHERE key = ?";
    SimpleStatement statement = SimpleStatement.newInstance(queryString);
    int expectedSize =
        9 // header
            + (4 + queryString.getBytes(Charsets.UTF_8).length) // query string
            + 2 // consistency level
            + 2 // serial consistency level
            + 4 // fetch size
            + 8 // timestamp
            + 4; // flags

    assertThat(v5SizeOf(statement)).isEqualTo(expectedSize);

    String value1 = "local";
    SimpleStatement statementWithAnonymousValue = SimpleStatement.newInstance(queryString, value1);
    assertThat(v5SizeOf(statementWithAnonymousValue))
        .isEqualTo(
            expectedSize
                + 2 // size of number of values
                + (4 + value1.getBytes(Charsets.UTF_8).length) // value
            );

    String key1 = "key";
    SimpleStatement statementWithNamedValue =
        SimpleStatement.newInstance(queryString, ImmutableMap.of(key1, value1));
    assertThat(v5SizeOf(statementWithNamedValue))
        .isEqualTo(
            expectedSize
                + 2 // size of number of values
                + (2 + key1.getBytes(Charsets.UTF_8).length) // key
                + (4 + value1.getBytes(Charsets.UTF_8).length) // value
            );

    SimpleStatement statementWithPagingState =
        statement.setPagingState(ByteBuffer.wrap(MOCK_PAGING_STATE));
    assertThat(v5SizeOf(statementWithPagingState))
        .isEqualTo(expectedSize + 4 + MOCK_PAGING_STATE.length);

    SimpleStatement statementWithPayload = statement.setCustomPayload(MOCK_PAYLOAD);
    assertThat(v5SizeOf(statementWithPayload))
        .isEqualTo(
            expectedSize
                + 2 // size of number of keys in the map
                // size of each key/value pair
                + (2 + "key1".getBytes(Charsets.UTF_8).length)
                + (4 + MOCK_PAYLOAD_VALUE1.remaining())
                + (2 + "key2".getBytes(Charsets.UTF_8).length)
                + (4 + MOCK_PAYLOAD_VALUE2.remaining()));

    SimpleStatement statementWithKeyspace = statement.setKeyspace("testKeyspace");
    assertThat(v5SizeOf(statementWithKeyspace))
        .isEqualTo(expectedSize + 2 + "testKeyspace".getBytes(Charsets.UTF_8).length);
  }

  @Test
  public void should_measure_size_of_bound_statement() {

    BoundStatement statement =
        newBoundStatement(
            preparedStatement,
            new ByteBuffer[] {ProtocolConstants.UNSET_VALUE, ProtocolConstants.UNSET_VALUE});

    int expectedSize =
        9 // header size
            + 4 // flags
            + 2 // consistency level
            + 2 // serial consistency level
            + 8 // timestamp
            + (2 + PREPARED_ID.length)
            + (2 + RESULT_METADATA_ID.length)
            + 2 // size of value list
            + 2 * 4 // two null values (size = -1)
            + 4 // fetch size
        ;
    assertThat(v5SizeOf(statement)).isEqualTo(expectedSize);

    BoundStatement withValues = statement.setInt(0, 0).setString(1, "test");
    expectedSize +=
        4 // the size of the int value
            + "test".getBytes(Charsets.UTF_8).length;
    assertThat(v5SizeOf(withValues)).isEqualTo(expectedSize);

    BoundStatement withPagingState = withValues.setPagingState(ByteBuffer.wrap(MOCK_PAGING_STATE));
    expectedSize += 4 + MOCK_PAGING_STATE.length;
    assertThat(v5SizeOf(withPagingState)).isEqualTo(expectedSize);

    BoundStatement withPayload = withPagingState.setCustomPayload(MOCK_PAYLOAD);
    expectedSize +=
        2 // size of number of keys in the map
            // size of each key/value pair
            + (2 + "key1".getBytes(Charsets.UTF_8).length)
            + (4 + MOCK_PAYLOAD_VALUE1.remaining())
            + (2 + "key2".getBytes(Charsets.UTF_8).length)
            + (4 + MOCK_PAYLOAD_VALUE2.remaining());
    assertThat(v5SizeOf(withPayload)).isEqualTo(expectedSize);
  }

  @Test
  public void should_measure_size_of_batch_statement() {
    String queryString = "SELECT release_version FROM system.local";
    String key1 = "key";
    String value1 = "value";
    SimpleStatement statement1 =
        SimpleStatement.newInstance(queryString, ImmutableMap.of(key1, value1));

    BoundStatement statement2 =
        newBoundStatement(
                preparedStatement,
                new ByteBuffer[] {ProtocolConstants.UNSET_VALUE, ProtocolConstants.UNSET_VALUE})
            .setInt(0, 0)
            .setString(1, "test");
    BoundStatement statement3 =
        newBoundStatement(
                preparedStatement,
                new ByteBuffer[] {ProtocolConstants.UNSET_VALUE, ProtocolConstants.UNSET_VALUE})
            .setInt(0, 0)
            .setString(1, "test2");

    BatchStatement batchStatement =
        BatchStatement.newInstance(DefaultBatchType.UNLOGGED)
            .add(statement1)
            .add(statement2)
            .add(statement3);

    int expectedSize =
        9 // header size
            + 1
            + 2 // batch type + number of queries
            // statements' type of id + id (query string/prepared id):
            + 1
            + (4 + queryString.getBytes(Charsets.UTF_8).length)
            + 1
            + (2 + PREPARED_ID.length)
            + 1
            + (2 + PREPARED_ID.length)
            // simple statement values
            + 2 // size of number of values
            + (2 + key1.getBytes(Charsets.UTF_8).length) // key
            + (4 + value1.getBytes(Charsets.UTF_8).length) // value
            // bound statements values
            + (2 + (4 + 4) + (4 + "test".getBytes(Charsets.UTF_8).length))
            + (2 + (4 + 4) + (4 + "test2".getBytes(Charsets.UTF_8).length))
            + 2 // consistency level
            + 2 // serial consistency level
            + 8 // timestamp
            + 4; // flags
    assertThat(v5SizeOf(batchStatement)).isEqualTo(expectedSize);

    BatchStatement withPayload = batchStatement.setCustomPayload(MOCK_PAYLOAD);
    expectedSize +=
        2 // size of number of keys in the map
            // size of each key/value pair
            + (2 + "key1".getBytes(Charsets.UTF_8).length)
            + (4 + MOCK_PAYLOAD_VALUE1.remaining())
            + (2 + "key2".getBytes(Charsets.UTF_8).length)
            + (4 + MOCK_PAYLOAD_VALUE2.remaining());
    assertThat(v5SizeOf(withPayload)).isEqualTo(expectedSize);
  }

  private int v5SizeOf(Statement statement) {
    return statement.computeSizeInBytes(driverContext);
  }

  private BoundStatement newBoundStatement(
      PreparedStatement preparedStatement, ByteBuffer[] initialValues) {
    return new DefaultBoundStatement(
        preparedStatement,
        preparedStatement.getVariableDefinitions(),
        initialValues,
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        -1,
        null,
        Integer.MIN_VALUE,
        null,
        null,
        null,
        CodecRegistry.DEFAULT,
        DefaultProtocolVersion.V5,
        null,
        Statement.NO_NOW_IN_SECONDS);
  }
}
