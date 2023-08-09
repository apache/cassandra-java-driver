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
package com.datastax.oss.driver.internal.core.data;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.metadata.token.ByteOrderedToken;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.nio.ByteBuffer;
import java.util.List;

public class ValuesHelper {

  public static ByteBuffer[] encodeValues(
      Object[] values,
      List<DataType> fieldTypes,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion) {
    Preconditions.checkArgument(
        values.length <= fieldTypes.size(),
        "Too many values (expected %s, got %s)",
        fieldTypes.size(),
        values.length);

    ByteBuffer[] encodedValues = new ByteBuffer[fieldTypes.size()];
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      ByteBuffer encodedValue;
      if (value instanceof Token) {
        if (value instanceof Murmur3Token) {
          encodedValue =
              TypeCodecs.BIGINT.encode(((Murmur3Token) value).getValue(), protocolVersion);
        } else if (value instanceof ByteOrderedToken) {
          encodedValue =
              TypeCodecs.BLOB.encode(((ByteOrderedToken) value).getValue(), protocolVersion);
        } else if (value instanceof RandomToken) {
          encodedValue =
              TypeCodecs.VARINT.encode(((RandomToken) value).getValue(), protocolVersion);
        } else {
          throw new IllegalArgumentException("Unsupported token type " + value.getClass());
        }
      } else {
        TypeCodec<Object> codec =
            (value == null)
                ? codecRegistry.codecFor(fieldTypes.get(i))
                : codecRegistry.codecFor(fieldTypes.get(i), value);
        encodedValue = codec.encode(value, protocolVersion);
      }
      encodedValues[i] = encodedValue;
    }
    return encodedValues;
  }

  public static ByteBuffer[] encodePreparedValues(
      Object[] values,
      ColumnDefinitions variableDefinitions,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion) {

    // Almost same as encodeValues, but we can't reuse because of variableDefinitions. Rebuilding a
    // list of datatypes is not worth it, so duplicate the code.

    Preconditions.checkArgument(
        values.length <= variableDefinitions.size(),
        "Too many variables (expected %s, got %s)",
        variableDefinitions.size(),
        values.length);

    ByteBuffer[] encodedValues = new ByteBuffer[variableDefinitions.size()];
    int i;
    for (i = 0; i < values.length; i++) {
      Object value = values[i];
      ByteBuffer encodedValue;
      if (value instanceof Token) {
        if (value instanceof Murmur3Token) {
          encodedValue =
              TypeCodecs.BIGINT.encode(((Murmur3Token) value).getValue(), protocolVersion);
        } else if (value instanceof ByteOrderedToken) {
          encodedValue =
              TypeCodecs.BLOB.encode(((ByteOrderedToken) value).getValue(), protocolVersion);
        } else if (value instanceof RandomToken) {
          encodedValue =
              TypeCodecs.VARINT.encode(((RandomToken) value).getValue(), protocolVersion);
        } else {
          throw new IllegalArgumentException("Unsupported token type " + value.getClass());
        }
      } else {
        TypeCodec<Object> codec =
            (value == null)
                ? codecRegistry.codecFor(variableDefinitions.get(i).getType())
                : codecRegistry.codecFor(variableDefinitions.get(i).getType(), value);
        encodedValue = codec.encode(value, protocolVersion);
      }
      encodedValues[i] = encodedValue;
    }
    for (; i < encodedValues.length; i++) {
      encodedValues[i] = ProtocolConstants.UNSET_VALUE;
    }
    return encodedValues;
  }

  public static ByteBuffer encodeToDefaultCqlMapping(
      Object value, CodecRegistry codecRegistry, ProtocolVersion protocolVersion) {
    if (value instanceof Token) {
      if (value instanceof Murmur3Token) {
        return TypeCodecs.BIGINT.encode(((Murmur3Token) value).getValue(), protocolVersion);
      } else if (value instanceof ByteOrderedToken) {
        return TypeCodecs.BLOB.encode(((ByteOrderedToken) value).getValue(), protocolVersion);
      } else if (value instanceof RandomToken) {
        return TypeCodecs.VARINT.encode(((RandomToken) value).getValue(), protocolVersion);
      } else {
        throw new IllegalArgumentException("Unsupported token type " + value.getClass());
      }
    } else {
      return codecRegistry.codecFor(value).encode(value, protocolVersion);
    }
  }
}
