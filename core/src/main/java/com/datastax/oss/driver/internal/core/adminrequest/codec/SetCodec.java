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
import java.util.LinkedHashSet;
import java.util.Set;

public class SetCodec<T> implements TypeCodec<Set<T>> {

  private final TypeCodec<T> elementCodec;

  public SetCodec(TypeCodec<T> elementCodec) {
    this.elementCodec = elementCodec;
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
      return result;
    }
  }

  @Override
  public Set<T> decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    Set<T> result = new LinkedHashSet<>();
    if (bytes != null && bytes.remaining() != 0) {
      int size = bytes.getInt();
      for (int i = 0; i < size; i++) {
        int elementSize = bytes.getInt();
        ByteBuffer encodedElement = bytes.slice();
        encodedElement.limit(elementSize);
        result.add(elementCodec.decode(encodedElement, protocolVersion));
      }
    }
    return result;
  }
}
