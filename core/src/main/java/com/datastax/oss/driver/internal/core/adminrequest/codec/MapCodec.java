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
package com.datastax.oss.driver.internal.core.adminrequest.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapCodec<K, V> implements TypeCodec<Map<K, V>> {

  private final TypeCodec<K> keyCodec;
  private final TypeCodec<V> valueCodec;

  public MapCodec(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec) {
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
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
      return result;
    }
  }

  @Override
  public Map<K, V> decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    Map<K, V> result = new LinkedHashMap<>();
    if (bytes != null && bytes.remaining() != 0) {
      int size = bytes.getInt();
      for (int i = 0; i < size; i++) {
        int keySize = bytes.getInt();
        ByteBuffer encodedKey = bytes.slice();
        encodedKey.limit(keySize);
        K key = keyCodec.decode(encodedKey, protocolVersion);
        int valueSize = bytes.getInt();
        ByteBuffer encodedValue = bytes.slice();
        encodedValue.limit(valueSize);
        V value = valueCodec.decode(encodedValue, protocolVersion);
        result.put(key, value);
      }
    }
    return result;
  }
}
