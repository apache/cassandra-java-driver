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
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.data.ValuesHelper;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.request.query.Values;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Sizes {

  /** Returns a common size for all kinds of Request implementations. */
  public static int minimumRequestSize(Request request) {

    // Header and payload are common inside a Frame at the protocol level

    // Frame header has a fixed size of 9 for protocol version >= V3, which includes Frame flags
    // size
    int size = FrameCodec.V3_ENCODED_HEADER_SIZE;

    if (!request.getCustomPayload().isEmpty()) {
      // Custom payload is not supported in v3, but assume user won't have a custom payload set if
      // they use this version
      size += PrimitiveSizes.sizeOfBytesMap(request.getCustomPayload());
    }

    return size;
  }

  public static int minimumStatementSize(Statement statement, DriverContext context) {
    int size = minimumRequestSize(statement);

    // These are options in the protocol inside a frame that are common to all Statements

    size += QueryOptions.queryFlagsSize(context.getProtocolVersion().getCode());

    size += PrimitiveSizes.SHORT; // size of consistency level
    size += PrimitiveSizes.SHORT; // size of serial consistency level

    return size;
  }

  /**
   * Returns the size in bytes of a simple statement's values, depending on whether the values are
   * named or positional.
   */
  public static int sizeOfSimpleStatementValues(
      SimpleStatement simpleStatement,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry) {
    int size = 0;

    if (!simpleStatement.getPositionalValues().isEmpty()) {

      List<ByteBuffer> positionalValues =
          new ArrayList<>(simpleStatement.getPositionalValues().size());
      for (Object value : simpleStatement.getPositionalValues()) {
        positionalValues.add(
            ValuesHelper.encodeToDefaultCqlMapping(value, codecRegistry, protocolVersion));
      }

      size += Values.sizeOfPositionalValues(positionalValues);

    } else if (!simpleStatement.getNamedValues().isEmpty()) {

      Map<String, ByteBuffer> namedValues = new HashMap<>(simpleStatement.getNamedValues().size());
      for (Map.Entry<CqlIdentifier, Object> value : simpleStatement.getNamedValues().entrySet()) {
        namedValues.put(
            value.getKey().asInternal(),
            ValuesHelper.encodeToDefaultCqlMapping(
                value.getValue(), codecRegistry, protocolVersion));
      }

      size += Values.sizeOfNamedValues(namedValues);
    }
    return size;
  }

  /** Return the size in bytes of a bound statement's values. */
  public static int sizeOfBoundStatementValues(BoundStatement boundStatement) {
    return Values.sizeOfPositionalValues(boundStatement.getValues());
  }

  /**
   * The size of a statement inside a batch query is different from the size of a complete
   * Statement. The inner batch statements only include the query or prepared ID, and the values of
   * the statement.
   */
  public static Integer sizeOfInnerBatchStatementInBytes(
      BatchableStatement statement, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    int size = 0;

    size +=
        PrimitiveSizes
            .BYTE; // for each inner statement, there is one byte for the "kind": prepared or string

    if (statement instanceof SimpleStatement) {
      size += PrimitiveSizes.sizeOfLongString(((SimpleStatement) statement).getQuery());
      size +=
          sizeOfSimpleStatementValues(
              ((SimpleStatement) statement), protocolVersion, codecRegistry);
    } else if (statement instanceof BoundStatement) {
      size +=
          PrimitiveSizes.sizeOfShortBytes(
              ((BoundStatement) statement).getPreparedStatement().getId());
      size += sizeOfBoundStatementValues(((BoundStatement) statement));
    }
    return size;
  }
}
