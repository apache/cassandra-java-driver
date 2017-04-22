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
package com.datastax.oss.driver.internal.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.type.DataType;
import com.datastax.oss.driver.api.type.SetType;
import com.datastax.oss.driver.api.type.codec.TypeCodec;
import com.datastax.oss.driver.api.type.reflect.GenericType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.Set;

public class SetCodec<T> implements TypeCodec<Set<T>> {

  private final DataType cqlType;
  private final GenericType<Set<T>> javaType;
  private final TypeCodec<T> elementCodec;

  public SetCodec(DataType cqlType, TypeCodec<T> elementCodec) {
    this.cqlType = cqlType;
    this.javaType = GenericType.setOf(elementCodec.getJavaType());
    this.elementCodec = elementCodec;
    Preconditions.checkArgument(cqlType instanceof SetType);
  }

  @Override
  public GenericType<Set<T>> getJavaType() {
    return javaType;
  }

  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean canEncode(Object value) {
    if (Set.class.isAssignableFrom(value.getClass())) {
      // runtime type ok, now check element type
      Set<?> set = (Set<?>) value;
      return set.isEmpty() || elementCodec.canEncode(set.iterator().next());
    } else {
      return false;
    }
  }

  @Override
  public ByteBuffer encode(Set<T> value, ProtocolVersion protocolVersion) {
    // An int indicating the number of elements in the set, followed by the elements. Each element
    // is a byte array representing the serialized value, preceded by an int indicating its size.
    if (value == null) {
      return null;
    } else {
      int i = 0;
      ByteBuffer[] encodedElements = new ByteBuffer[value.size()];
      int toAllocate = 4; // initialize with number of elements
      for (T element : value) {
        if (element == null) {
          throw new NullPointerException("Collection elements cannot be null");
        }
        ByteBuffer encodedElement;
        try {
          encodedElement = elementCodec.encode(element, protocolVersion);
        } catch (ClassCastException e) {
          throw new IllegalArgumentException("Invalid type for element: " + element.getClass());
        }
        encodedElements[i++] = encodedElement;
        toAllocate += 4 + encodedElement.remaining(); // the element preceded by its size
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
  public Set<T> decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return new LinkedHashSet<>(0);
    } else {
      ByteBuffer input = bytes.duplicate();
      int size = input.getInt();
      Set<T> result = Sets.newLinkedHashSetWithExpectedSize(size);
      for (int i = 0; i < size; i++) {
        int elementSize = input.getInt();
        ByteBuffer encodedElement = input.slice();
        encodedElement.limit(elementSize);
        input.position(input.position() + elementSize);
        result.add(elementCodec.decode(encodedElement, protocolVersion));
      }
      return result;
    }
  }

  @Override
  public String format(Set<T> value) {
    if (value == null) {
      return "NULL";
    }
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (T t : value) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(elementCodec.format(t));
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public Set<T> parse(String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;

    int idx = ParseUtils.skipSpaces(value, 0);
    if (value.charAt(idx++) != '{')
      throw new IllegalArgumentException(
          String.format(
              "Cannot parse set value from \"%s\", at character %d expecting '{' but got '%c'",
              value, idx, value.charAt(idx)));

    idx = ParseUtils.skipSpaces(value, idx);

    if (value.charAt(idx) == '}') {
      return new LinkedHashSet<>(0);
    }

    Set<T> set = new LinkedHashSet<>();
    while (idx < value.length()) {
      int n;
      try {
        n = ParseUtils.skipCQLValue(value, idx);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse set value from \"%s\", invalid CQL value at character %d",
                value, idx),
            e);
      }

      set.add(elementCodec.parse(value.substring(idx, n)));
      idx = n;

      idx = ParseUtils.skipSpaces(value, idx);
      if (value.charAt(idx) == '}') return set;
      if (value.charAt(idx++) != ',')
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse set value from \"%s\", at character %d expecting ',' but got '%c'",
                value, idx, value.charAt(idx)));

      idx = ParseUtils.skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format("Malformed set value \"%s\", missing closing '}'", value));
  }
}
