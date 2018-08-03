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
package com.datastax.driver.core;

import static com.datastax.driver.core.Assertions.assertThat;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StatementSizeTest {

  private static final byte[] MOCK_PAGING_STATE = Bytes.getArray(Bytes.fromHexString("0xdeadbeef"));
  private static final ByteBuffer MOCK_PAYLOAD_VALUE1 = Bytes.fromHexString("0xabcd");
  private static final ByteBuffer MOCK_PAYLOAD_VALUE2 = Bytes.fromHexString("0xef");
  private static final ImmutableMap<String, ByteBuffer> MOCK_PAYLOAD =
      ImmutableMap.of("key1", MOCK_PAYLOAD_VALUE1, "key2", MOCK_PAYLOAD_VALUE2);
  private static final byte[] PREPARED_ID = Bytes.getArray(Bytes.fromHexString("0xaaaa"));
  private static final byte[] RESULT_METADATA_ID = Bytes.getArray(Bytes.fromHexString("0xbbbb"));

  @Mock private PreparedStatement preparedStatement;

  @BeforeMethod(groups = {"unit"})
  public void setup() {
    MockitoAnnotations.initMocks(this);

    PreparedId preparedId =
        new PreparedId(
            new PreparedId.PreparedMetadata(MD5Digest.wrap(PREPARED_ID), null),
            new PreparedId.PreparedMetadata(MD5Digest.wrap(RESULT_METADATA_ID), null),
            new int[0],
            ProtocolVersion.V5);
    Mockito.when(preparedStatement.getPreparedId()).thenReturn(preparedId);

    ColumnDefinitions columnDefinitions =
        new ColumnDefinitions(
            new Definition[] {
              new Definition("ks", "table", "c1", DataType.cint()),
              new Definition("ks", "table", "c2", DataType.text())
            },
            CodecRegistry.DEFAULT_INSTANCE);
    Mockito.when(preparedStatement.getVariables()).thenReturn(columnDefinitions);
    Mockito.when(preparedStatement.getIncomingPayload()).thenReturn(null);
    Mockito.when(preparedStatement.getOutgoingPayload()).thenReturn(null);
    Mockito.when(preparedStatement.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);
  }

  @Test(groups = "unit")
  public void should_measure_size_of_simple_statement() {
    String queryString = "SELECT release_version FROM system.local WHERE key = ?";
    SimpleStatement statement = new SimpleStatement(queryString);
    int expectedSize =
        9 // header
            + (4 + queryString.getBytes(Charsets.UTF_8).length) // query string
            + 2 // consistency level
            + 2 // serial consistency level
            + 4 // fetch size
            + 8 // timestamp
            + 4; // flags
    assertThat(v5SizeOf(statement)).isEqualTo(expectedSize);

    SimpleStatement statementWithAnonymousValue =
        new SimpleStatement(statement.getQueryString(), "local");
    assertThat(v5SizeOf(statementWithAnonymousValue))
        .isEqualTo(
            expectedSize
                + 2 // size of number of values
                + (4 + "local".getBytes(Charsets.UTF_8).length) // value
            );

    SimpleStatement statementWithNamedValue =
        new SimpleStatement(
            statement.getQueryString(), ImmutableMap.<String, Object>of("key", "local"));
    assertThat(v5SizeOf(statementWithNamedValue))
        .isEqualTo(
            expectedSize
                + 2 // size of number of values
                + (2 + "key".getBytes(Charsets.UTF_8).length) // key
                + (4 + "local".getBytes(Charsets.UTF_8).length) // value
            );

    statement.setPagingStateUnsafe(MOCK_PAGING_STATE);
    expectedSize += 4 + MOCK_PAGING_STATE.length;
    assertThat(v5SizeOf(statement)).isEqualTo(expectedSize);

    statement.setOutgoingPayload(MOCK_PAYLOAD);
    expectedSize +=
        2 // size of number of keys in the map
            // size of each key/value pair
            + (2 + "key1".getBytes(Charsets.UTF_8).length)
            + (4 + MOCK_PAYLOAD_VALUE1.remaining())
            + (2 + "key2".getBytes(Charsets.UTF_8).length)
            + (4 + MOCK_PAYLOAD_VALUE2.remaining());
    assertThat(v5SizeOf(statement)).isEqualTo(expectedSize);
  }

  @Test(groups = "unit")
  public void should_measure_size_of_bound_statement() {
    BoundStatement statement = new BoundStatement(preparedStatement);
    int expectedSize =
        9 // header size
            + (2 + PREPARED_ID.length)
            + (2 + RESULT_METADATA_ID.length)
            + 2 // consistency level
            + 2 // serial consistency level
            + 4 // fetch size
            + 8 // timestamp
            + 4 // flags
            + 2 // size of value list
            + 2 * (4) // two null values (size = -1)
        ;
    assertThat(v5SizeOf(statement)).isEqualTo(expectedSize);

    statement.setInt(0, 0);
    expectedSize += 4; // serialized value (we already have its size from when it was null above)
    statement.setString(1, "test");
    expectedSize += "test".getBytes(Charsets.UTF_8).length;
    assertThat(v5SizeOf(statement)).isEqualTo(expectedSize);

    statement.setPagingStateUnsafe(MOCK_PAGING_STATE);
    expectedSize += 4 + MOCK_PAGING_STATE.length;
    assertThat(v5SizeOf(statement)).isEqualTo(expectedSize);

    statement.setOutgoingPayload(MOCK_PAYLOAD);
    expectedSize +=
        2 // size of number of keys in the map
            // size of each key/value pair
            + (2 + "key1".getBytes(Charsets.UTF_8).length)
            + (4 + MOCK_PAYLOAD_VALUE1.remaining())
            + (2 + "key2".getBytes(Charsets.UTF_8).length)
            + (4 + MOCK_PAYLOAD_VALUE2.remaining());
    assertThat(v5SizeOf(statement)).isEqualTo(expectedSize);
  }

  @Test(groups = "unit")
  public void should_measure_size_of_batch_statement() {
    String queryString = "SELECT release_version FROM system.local";
    SimpleStatement statement1 = new SimpleStatement(queryString);

    BoundStatement statement2 =
        new BoundStatement(preparedStatement).setInt(0, 0).setString(1, "test");
    BoundStatement statement3 =
        new BoundStatement(preparedStatement).setInt(0, 0).setString(1, "test2");

    BatchStatement batchStatement =
        new BatchStatement().add(statement1).add(statement2).add(statement3);

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
            // value lists
            + 2
            + (2 + (4 + 4) + (4 + "test".getBytes(Charsets.UTF_8).length))
            + (2 + (4 + 4) + (4 + "test2".getBytes(Charsets.UTF_8).length))
            + 2 // consistency level
            + 2 // serial consistency level
            + 8 // client timestamp
            + 4; // flags
    assertThat(v5SizeOf(batchStatement)).isEqualTo(expectedSize);

    batchStatement.setOutgoingPayload(MOCK_PAYLOAD);
    expectedSize +=
        2 // size of number of keys in the map
            // size of each key/value pair
            + (2 + "key1".getBytes(Charsets.UTF_8).length)
            + (4 + MOCK_PAYLOAD_VALUE1.remaining())
            + (2 + "key2".getBytes(Charsets.UTF_8).length)
            + (4 + MOCK_PAYLOAD_VALUE2.remaining());
    assertThat(v5SizeOf(batchStatement)).isEqualTo(expectedSize);
  }

  @Test(groups = "unit")
  public void should_measure_size_of_wrapped_statement() {
    String queryString = "SELECT release_version FROM system.local WHERE key = ?";
    Statement statement = new StatementWrapper(new SimpleStatement(queryString)) {};
    int expectedSize =
        9 // header
            + (4 + queryString.getBytes(Charsets.UTF_8).length) // query string
            + 2 // consistency level
            + 2 // serial consistency level
            + 4 // fetch size
            + 8 // timestamp
            + 4; // flags
    assertThat(v5SizeOf(statement)).isEqualTo(expectedSize);
  }

  private int v5SizeOf(Statement statement) {
    return statement.requestSizeInBytes(ProtocolVersion.V5, CodecRegistry.DEFAULT_INSTANCE);
  }
}
