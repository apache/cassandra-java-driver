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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Deterministic (non-timing-based) regression test for the SKIP_METADATA / CASSANDRA-10786 race:
 * execution A's request is encoded while the prepared statement's cached result metadata is
 * OLD_DEFS, then a concurrent execution B's response arrives first and swaps the shared {@link
 * DefaultPreparedStatement} metadata to NEW_DEFS before A's own SKIP_METADATA response is decoded.
 *
 * <p>NEW_DEFS here models a real {@code ALTER TABLE ... ADD extra_0 bigint} (as issued by the live
 * harness in {@code MetadataRaceHarness}): Cassandra returns non-primary-key columns in
 * alphabetical order, so the added column ("extra_0") sorts before the existing ones and is
 * inserted at index 0, shifting "group_id", "is_deleted" and "last_modified" each one slot to the
 * right. Their relative order to each other is unchanged -- ALTER TABLE can only insert a column,
 * it cannot reorder existing ones -- but their absolute indices shift, which is exactly what used
 * to corrupt decoding of A's already-encoded, old-layout row bytes before the fix.
 *
 * <p>The fix snapshots the metadata at encode time and threads it through to decode time (see
 * {@link Conversions#getResultDefinitions}), so A correctly decodes against OLD_DEFS regardless of
 * B's concurrent swap. Response arrival order here is controlled by direct, sequential calls into
 * {@code getResultDefinitions}, so the scenario triggers identically on every run.
 */
public class ConversionsMetadataRaceTest {

  private static final ByteBuffer OLD_RESULT_METADATA_ID = Bytes.fromHexString("0x01");
  private static final ByteBuffer NEW_RESULT_METADATA_ID = Bytes.fromHexString("0x02");

  @Test
  public void should_decode_skip_metadata_response_using_encode_time_snapshot() {
    InternalDriverContext context = Mockito.mock(InternalDriverContext.class);
    Mockito.when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    Mockito.when(context.getProtocolVersion()).thenReturn(DefaultProtocolVersion.V4);

    // OLD_DEFS: the layout in effect when A's and B's requests were encoded (SKIP_METADATA
    // decided here). Regular (non-PK) columns come back from Cassandra in alphabetical order, so
    // for the schema "group_id, is_deleted, last_modified" that's also declaration order here.
    ColumnDefinitions oldDefs =
        Conversions.toColumnDefinitions(
            new RowsMetadata(
                ImmutableList.of(
                    columnSpec("group_id", 0, ProtocolConstants.DataType.VARCHAR),
                    columnSpec("is_deleted", 1, ProtocolConstants.DataType.BOOLEAN),
                    columnSpec("last_modified", 2, ProtocolConstants.DataType.BIGINT)),
                null,
                new int[0],
                null),
            context);

    DefaultPreparedStatement preparedStatement =
        new DefaultPreparedStatement(
            Bytes.fromHexString("0xAAAA"),
            "SELECT * FROM t2 WHERE k = ?",
            DefaultColumnDefinitions.valueOf(Collections.emptyList()),
            Collections.emptyList(),
            OLD_RESULT_METADATA_ID,
            oldDefs,
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
            5000,
            null,
            null,
            false,
            CodecRegistry.DEFAULT,
            DefaultProtocolVersion.V4);

    // Two concurrent executions of the same cached PreparedStatement, both encoded while OLD_DEFS
    // is current -- so both capture the same encode-time snapshot, exactly as
    // CqlRequestHandler.sendRequest does for each execution before calling Conversions.toMessage.
    BoundStatement stmtA = preparedStatement.bind();
    BoundStatement stmtB = preparedStatement.bind();
    DefaultPreparedStatement.ResultMetadata encodeTimeSnapshot =
        preparedStatement.getCurrentResultMetadata();

    // B's response lands first: the server signals a schema change (CASSANDRA-10786) triggered by
    // a real "ALTER TABLE t2 ADD extra_0 bigint". Non-PK columns are returned alphabetically, so
    // "extra_0" sorts before "group_id" and is inserted at index 0 -- shifting the three existing
    // columns one slot to the right, without reordering them relative to each other. Decoding this
    // response swaps the prepared statement's shared cached metadata to NEW_DEFS.
    Rows rowsB =
        new DefaultRows(
            new RowsMetadata(
                ImmutableList.of(
                    columnSpec("extra_0", 0, ProtocolConstants.DataType.BIGINT),
                    columnSpec("group_id", 1, ProtocolConstants.DataType.VARCHAR),
                    columnSpec("is_deleted", 2, ProtocolConstants.DataType.BOOLEAN),
                    columnSpec("last_modified", 3, ProtocolConstants.DataType.BIGINT)),
                null,
                new int[0],
                Bytes.getArray(NEW_RESULT_METADATA_ID)),
            oneRow(
                TypeCodecs.BIGINT.encode(0L, DefaultProtocolVersion.V4),
                TypeCodecs.TEXT.encode("g1", DefaultProtocolVersion.V4),
                TypeCodecs.BOOLEAN.encode(true, DefaultProtocolVersion.V4),
                TypeCodecs.BIGINT.encode(1_700_000_000_000L, DefaultProtocolVersion.V4)));

    ColumnDefinitions defsForB =
        Conversions.getResultDefinitions(rowsB, stmtB, context, encodeTimeSnapshot);
    assertThat(defsForB.get(2).getName().asInternal()).isEqualTo("is_deleted");

    // The swap happened as a side effect of decoding B: the shared cache now holds NEW_DEFS.
    assertThat(preparedStatement.getResultMetadataId()).isEqualTo(NEW_RESULT_METADATA_ID);

    // A's response arrives and is decoded now: SKIP_METADATA, row bytes laid out per OLD_DEFS
    // (group_id text, is_deleted boolean, last_modified bigint) -- only 3 values, since A's
    // request was encoded before the ALTER TABLE added "extra_0".
    Rows rowsA =
        new DefaultRows(
            new RowsMetadata(oldDefs.size(), null, new int[0], null),
            oneRow(
                TypeCodecs.TEXT.encode("g2", DefaultProtocolVersion.V4),
                TypeCodecs.BOOLEAN.encode(false, DefaultProtocolVersion.V4),
                TypeCodecs.BIGINT.encode(1_700_000_000_001L, DefaultProtocolVersion.V4)));

    ColumnDefinitions defsForA =
        Conversions.getResultDefinitions(rowsA, stmtA, context, encodeTimeSnapshot);

    // Fixed behaviour: A decodes against its own encode-time snapshot (OLD_DEFS, 3 columns), not
    // the shared cache's current state (NEW_DEFS, 4 columns), even though B already swapped it.
    assertThat(defsForA).isEqualTo(oldDefs);
    assertThat(defsForA.get(1).getName().asInternal()).isEqualTo("is_deleted");

    Row row = new DefaultRow(defsForA, rowsA.getData().poll(), context);

    // No column-offset shift: OLD_DEFS says index 1 ("is_deleted") is a 1-byte boolean, and A's
    // bytes at that slot really are the 1-byte boolean OLD_DEFS put there.
    assertThatCode(() -> row.getBoolean("is_deleted")).doesNotThrowAnyException();
    assertThat(row.getBoolean("is_deleted")).isFalse();
    assertThat(row.getLong("last_modified")).isEqualTo(1_700_000_000_001L);
  }

  private static ColumnSpec columnSpec(String name, int index, int protocolDataType) {
    return new ColumnSpec("race_test", "t2", name, index, RawType.PRIMITIVES.get(protocolDataType));
  }

  private static Queue<List<ByteBuffer>> oneRow(ByteBuffer... values) {
    Queue<List<ByteBuffer>> data = new ArrayDeque<>();
    data.add(ImmutableList.copyOf(values));
    return data;
  }
}
