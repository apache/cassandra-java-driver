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
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ListCodec<T> implements TypeCodec<List<T>> {

  private final DataType cqlType;
  private final GenericType<List<T>> javaType;
  private final TypeCodec<T> elementCodec;

  public ListCodec(DataType cqlType, TypeCodec<T> elementCodec) {
    this.cqlType = cqlType;
    this.javaType = GenericType.listOf(elementCodec.getJavaType());
    this.elementCodec = elementCodec;
    Preconditions.checkArgument(cqlType instanceof ListType);
  }

  @Override
  public GenericType<List<T>> getJavaType() {
    return javaType;
  }

  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean accepts(Object value) {
    if (List.class.isAssignableFrom(value.getClass())) {
      // runtime type ok, now check element type
      List<?> list = (List<?>) value;
      return list.isEmpty() || elementCodec.accepts(list.get(0));
    } else {
      return false;
    }
  }

  @Override
  public ByteBuffer encode(List<T> value, ProtocolVersion protocolVersion) {
    // An int indicating the number of elements in the list, followed by the elements. Each element
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
  public List<T> decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return new ArrayList<>(0);
    } else {
      ByteBuffer input = bytes.duplicate();
      int size = input.getInt();
      List<T> result = new ArrayList<>(size);
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
  public String format(List<T> value) {
    if (value == null) {
      return "NULL";
    }
    StringBuilder sb = new StringBuilder("[");
    boolean first = true;
    for (T t : value) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(elementCodec.format(t));
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public List<T> parse(String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;

    int idx = ParseUtils.skipSpaces(value, 0);
    if (value.charAt(idx++) != '[')
      throw new IllegalArgumentException(
          String.format(
              "Cannot parse list value from \"%s\", at character %d expecting '[' but got '%c'",
              value, idx, value.charAt(idx)));

    idx = ParseUtils.skipSpaces(value, idx);

    if (value.charAt(idx) == ']') {
      return new ArrayList<>(0);
    }

    List<T> list = new ArrayList<>();
    while (idx < value.length()) {
      int n;
      try {
        n = ParseUtils.skipCQLValue(value, idx);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse list value from \"%s\", invalid CQL value at character %d",
                value, idx),
            e);
      }

      list.add(elementCodec.parse(value.substring(idx, n)));
      idx = n;

      idx = ParseUtils.skipSpaces(value, idx);
      if (value.charAt(idx) == ']') return list;
      if (value.charAt(idx++) != ',')
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse list value from \"%s\", at character %d expecting ',' but got '%c'",
                value, idx, value.charAt(idx)));

      idx = ParseUtils.skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format("Malformed list value \"%s\", missing closing ']'", value));
  }
}
