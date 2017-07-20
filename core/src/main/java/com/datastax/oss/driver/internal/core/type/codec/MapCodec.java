/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapCodec<K, V> implements TypeCodec<Map<K, V>> {

  private final DataType cqlType;
  private final GenericType<Map<K, V>> javaType;
  private final TypeCodec<K> keyCodec;
  private final TypeCodec<V> valueCodec;

  public MapCodec(DataType cqlType, TypeCodec<K> keyCodec, TypeCodec<V> valueCodec) {
    this.cqlType = cqlType;
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
    this.javaType = GenericType.mapOf(keyCodec.getJavaType(), valueCodec.getJavaType());
  }

  @Override
  public GenericType<Map<K, V>> getJavaType() {
    return javaType;
  }

  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean accepts(Object value) {
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
  public ByteBuffer encode(Map<K, V> value, ProtocolVersion protocolVersion) {
    // An int indicating the number of key/value pairs in the map, followed by the pairs. Each pair
    // is a byte array representing the serialized key, preceded by an int indicating its size,
    // followed by the value in the same format.
    if (value == null) {
      return null;
    } else {
      int i = 0;
      ByteBuffer[] encodedElements = new ByteBuffer[value.size() * 2];
      int toAllocate = 4; // initialize with number of elements
      for (Map.Entry<K, V> entry : value.entrySet()) {
        if (entry.getValue() == null) {
          throw new NullPointerException("Collection elements cannot be null");
        }
        ByteBuffer encodedKey;
        try {
          encodedKey = keyCodec.encode(entry.getKey(), protocolVersion);
        } catch (ClassCastException e) {
          throw new IllegalArgumentException("Invalid type for key: " + entry.getKey().getClass());
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

  @Override
  public Map<K, V> decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return new LinkedHashMap<>(0);
    } else {
      ByteBuffer input = bytes.duplicate();
      int size = input.getInt();
      Map<K, V> result = Maps.newLinkedHashMapWithExpectedSize(size);
      for (int i = 0; i < size; i++) {
        int keySize = input.getInt();
        ByteBuffer encodedKey = input.slice();
        encodedKey.limit(keySize);
        input.position(input.position() + keySize);
        K key = keyCodec.decode(encodedKey, protocolVersion);

        int valueSize = input.getInt();
        ByteBuffer encodedValue = input.slice();
        encodedValue.limit(valueSize);
        input.position(input.position() + valueSize);
        V value = valueCodec.decode(encodedValue, protocolVersion);

        result.put(key, value);
      }
      return result;
    }
  }

  @Override
  public String format(Map<K, V> value) {
    if (value == null) {
      return "NULL";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    boolean first = true;
    for (Map.Entry<K, V> e : value.entrySet()) {
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

  @Override
  public Map<K, V> parse(String value) {
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

    Map<K, V> map = new LinkedHashMap<>();
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

      K k = keyCodec.parse(value.substring(idx, n));
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

      V v = valueCodec.parse(value.substring(idx, n));
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
