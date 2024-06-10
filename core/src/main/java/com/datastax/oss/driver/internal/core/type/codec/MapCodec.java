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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class MapCodec<KeyT, ValueT> implements TypeCodec<Map<KeyT, ValueT>> {

  private final DataType cqlType;
  private final GenericType<Map<KeyT, ValueT>> javaType;
  private final TypeCodec<KeyT> keyCodec;
  private final TypeCodec<ValueT> valueCodec;

  public MapCodec(DataType cqlType, TypeCodec<KeyT> keyCodec, TypeCodec<ValueT> valueCodec) {
    this.cqlType = cqlType;
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
    this.javaType = GenericType.mapOf(keyCodec.getJavaType(), valueCodec.getJavaType());
  }

  @NonNull
  @Override
  public GenericType<Map<KeyT, ValueT>> getJavaType() {
    return javaType;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    if (value instanceof Map) {
      // runtime type ok, now check key and value types
      Map<?, ?> map = (Map<?, ?>) value;
      if (map.isEmpty()) {
        return true;
      }
      Map.Entry<?, ?> entry = map.entrySet().iterator().next();
      return keyCodec.accepts(entry.getKey()) && valueCodec.accepts(entry.getValue());
    }
    return false;
  }

  @Override
  @Nullable
  public ByteBuffer encode(
      @Nullable Map<KeyT, ValueT> value, @NonNull ProtocolVersion protocolVersion) {
    // An int indicating the number of key/value pairs in the map, followed by the pairs. Each pair
    // is a byte array representing the serialized key, preceded by an int indicating its size,
    // followed by the value in the same format.
    if (value == null) {
      return null;
    } else {
      int i = 0;
      ByteBuffer[] encodedElements = new ByteBuffer[value.size() * 2];
      int toAllocate = 4; // initialize with number of elements
      for (Map.Entry<KeyT, ValueT> entry : value.entrySet()) {
        if (entry.getKey() == null) {
          throw new NullPointerException("Map keys cannot be null");
        }
        if (entry.getValue() == null) {
          throw new NullPointerException("Map values cannot be null");
        }
        ByteBuffer encodedKey;
        try {
          encodedKey = keyCodec.encode(entry.getKey(), protocolVersion);
        } catch (ClassCastException e) {
          throw new IllegalArgumentException("Invalid type for key: " + entry.getKey().getClass());
        }
        if (encodedKey == null) {
          throw new NullPointerException("Map keys cannot encode to CQL NULL");
        }
        encodedElements[i++] = encodedKey;
        toAllocate += 4 + encodedKey.remaining(); // the key preceded by its size
        ByteBuffer encodedValue;
        try {
          encodedValue = valueCodec.encode(entry.getValue(), protocolVersion);
        } catch (ClassCastException e) {
          throw new IllegalArgumentException(
              "Invalid type for value: " + entry.getValue().getClass());
        }
        if (encodedValue == null) {
          throw new NullPointerException("Map values cannot encode to CQL NULL");
        }
        encodedElements[i++] = encodedValue;
        toAllocate += 4 + encodedValue.remaining(); // the value preceded by its size
      }
      ByteBuffer result = ByteBuffer.allocate(toAllocate);
      result.putInt(value.size());
      for (ByteBuffer encodedElement : encodedElements) {
        result.putInt(encodedElement.remaining());
        result.put(encodedElement);
      }
      result.flip();
      return result;
    }
  }

  @Nullable
  @Override
  public Map<KeyT, ValueT> decode(
      @Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return new LinkedHashMap<>(0);
    } else {
      ByteBuffer input = bytes.duplicate();
      int size = input.getInt();
      Map<KeyT, ValueT> result = Maps.newLinkedHashMapWithExpectedSize(size);
      for (int i = 0; i < size; i++) {
        KeyT key;
        int keySize = input.getInt();
        // Allow null elements on the decode path, because Cassandra might return such collections
        // for some computed values in the future -- e.g. SELECT ttl(some_collection)
        if (keySize < 0) {
          key = null;
        } else {
          ByteBuffer encodedKey = input.slice();
          encodedKey.limit(keySize);
          key = keyCodec.decode(encodedKey, protocolVersion);
          input.position(input.position() + keySize);
        }
        ValueT value;
        int valueSize = input.getInt();
        if (valueSize < 0) {
          value = null;
        } else {
          ByteBuffer encodedValue = input.slice();
          encodedValue.limit(valueSize);
          value = valueCodec.decode(encodedValue, protocolVersion);
          input.position(input.position() + valueSize);
        }
        result.put(key, value);
      }
      return result;
    }
  }

  @NonNull
  @Override
  public String format(@Nullable Map<KeyT, ValueT> value) {
    if (value == null) {
      return "NULL";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    boolean first = true;
    for (Map.Entry<KeyT, ValueT> e : value.entrySet()) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(keyCodec.format(e.getKey()));
      sb.append(":");
      sb.append(valueCodec.format(e.getValue()));
    }
    sb.append("}");
    return sb.toString();
  }

  @Nullable
  @Override
  public Map<KeyT, ValueT> parse(@Nullable String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }

    int idx = ParseUtils.skipSpaces(value, 0);
    if (value.charAt(idx++) != '{') {
      throw new IllegalArgumentException(
          String.format(
              "cannot parse map value from \"%s\", at character %d expecting '{' but got '%c'",
              value, idx, value.charAt(idx)));
    }

    idx = ParseUtils.skipSpaces(value, idx);

    if (value.charAt(idx) == '}') {
      return new LinkedHashMap<>(0);
    }

    Map<KeyT, ValueT> map = new LinkedHashMap<>();
    while (idx < value.length()) {
      int n;
      try {
        n = ParseUtils.skipCQLValue(value, idx);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse map value from \"%s\", invalid CQL value at character %d",
                value, idx),
            e);
      }

      KeyT k = keyCodec.parse(value.substring(idx, n));
      idx = n;

      idx = ParseUtils.skipSpaces(value, idx);
      if (value.charAt(idx++) != ':') {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse map value from \"%s\", at character %d expecting ':' but got '%c'",
                value, idx, value.charAt(idx)));
      }
      idx = ParseUtils.skipSpaces(value, idx);

      try {
        n = ParseUtils.skipCQLValue(value, idx);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse map value from \"%s\", invalid CQL value at character %d",
                value, idx),
            e);
      }

      ValueT v = valueCodec.parse(value.substring(idx, n));
      idx = n;

      map.put(k, v);

      idx = ParseUtils.skipSpaces(value, idx);
      if (value.charAt(idx) == '}') {
        return map;
      }
      if (value.charAt(idx++) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse map value from \"%s\", at character %d expecting ',' but got '%c'",
                value, idx, value.charAt(idx)));
      }

      idx = ParseUtils.skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format("Malformed map value \"%s\", missing closing '}'", value));
  }
}
