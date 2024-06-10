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
package com.datastax.oss.driver.internal.core.type.codec.extras.array;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * Codec dealing with Java object arrays. Serialization and deserialization of elements in the array
 * is delegated to the provided element codec.
 *
 * <p>For example, to create a codec that maps {@code list<text>} to {@code String[]}, declare the
 * following:
 *
 * <pre>{@code
 * ObjectListToArrayCodec<String> stringArrayCodec = new ObjectListToArrayCodec<>(TypeCodecs.TEXT);
 * }</pre>
 *
 * @param <ElementT> The Java array component type this codec handles
 */
@Immutable
public class ObjectListToArrayCodec<ElementT> extends AbstractListToArrayCodec<ElementT[]> {

  private final TypeCodec<ElementT> elementCodec;

  public ObjectListToArrayCodec(@NonNull TypeCodec<ElementT> elementCodec) {
    super(
        DataTypes.listOf(
            Objects.requireNonNull(elementCodec, "elementCodec must not be null").getCqlType()),
        GenericType.arrayOf(elementCodec.getJavaType()));
    this.elementCodec = elementCodec;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    Objects.requireNonNull(value);
    Class<?> clazz = value.getClass();
    return clazz.isArray()
        && clazz.getComponentType().equals(elementCodec.getJavaType().getRawType());
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable ElementT[] value, @NonNull ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    int i = 0;
    ByteBuffer[] encodedElements = new ByteBuffer[value.length];
    int toAllocate = 4; // initialize with number of elements
    for (ElementT elt : value) {
      if (elt == null) {
        throw new NullPointerException("Collection elements cannot be null");
      }
      ByteBuffer encodedElement;
      try {
        encodedElement = elementCodec.encode(elt, protocolVersion);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid type for %s element, expecting %s but got %s",
                cqlType, elementCodec.getJavaType(), elt.getClass()),
            e);
      }
      if (encodedElement == null) {
        throw new NullPointerException("Collection elements cannot encode to CQL NULL");
      }
      encodedElements[i++] = encodedElement;
      toAllocate += 4 + encodedElement.remaining(); // the element preceded by its size
    }
    ByteBuffer result = ByteBuffer.allocate(toAllocate);
    result.putInt(value.length);
    for (ByteBuffer encodedElement : encodedElements) {
      result.putInt(encodedElement.remaining());
      result.put(encodedElement);
    }
    result.flip();
    return result;
  }

  @Nullable
  @Override
  public ElementT[] decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return newInstance(0);
    }
    ByteBuffer input = bytes.duplicate();
    int size = input.getInt();
    ElementT[] result = newInstance(size);
    for (int i = 0; i < size; i++) {
      ElementT element;
      int elementSize = input.getInt();
      // Allow null elements on the decode path, because Cassandra might return such collections
      // for some computed values in the future -- e.g. SELECT ttl(some_collection)
      if (elementSize < 0) {
        element = null;
      } else {
        ByteBuffer encodedElement = input.slice();
        encodedElement.limit(elementSize);
        element = elementCodec.decode(encodedElement, protocolVersion);
        input.position(input.position() + elementSize);
      }
      result[i] = element;
    }
    return result;
  }

  @Override
  protected void formatElement(
      @NonNull StringBuilder output, @NonNull ElementT[] array, int index) {
    output.append(elementCodec.format(array[index]));
  }

  @Override
  protected void parseElement(@NonNull String input, @NonNull ElementT[] array, int index) {
    array[index] = elementCodec.parse(input);
  }

  @NonNull
  @Override
  @SuppressWarnings("unchecked")
  protected ElementT[] newInstance(int size) {
    return (ElementT[]) Array.newInstance(getJavaType().getRawType().getComponentType(), size);
  }
}
