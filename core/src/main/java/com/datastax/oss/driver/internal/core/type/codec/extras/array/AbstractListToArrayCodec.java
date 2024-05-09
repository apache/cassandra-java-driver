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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.ParseUtils;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.Array;
import java.util.Objects;

/**
 * Base class for all codecs dealing with Java arrays. This class aims to reduce the amount of code
 * required to create such codecs.
 *
 * @param <ArrayT> The Java array type this codec handles
 */
public abstract class AbstractListToArrayCodec<ArrayT> implements TypeCodec<ArrayT> {

  @NonNull protected final ListType cqlType;
  @NonNull protected final GenericType<ArrayT> javaType;

  /**
   * @param cqlType The CQL type. Must be a list type.
   * @param arrayType The Java type. Must be an array class.
   */
  protected AbstractListToArrayCodec(
      @NonNull ListType cqlType, @NonNull GenericType<ArrayT> arrayType) {
    this.cqlType = Objects.requireNonNull(cqlType, "cqlType cannot be null");
    this.javaType = Objects.requireNonNull(arrayType, "arrayType cannot be null");
    if (!arrayType.isArray()) {
      throw new IllegalArgumentException("Expecting Java array class, got " + arrayType);
    }
  }

  @NonNull
  @Override
  public GenericType<ArrayT> getJavaType() {
    return javaType;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @NonNull
  @Override
  public String format(@Nullable ArrayT array) {
    if (array == null) {
      return "NULL";
    }
    int length = Array.getLength(array);
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = 0; i < length; i++) {
      if (i != 0) {
        sb.append(",");
      }
      formatElement(sb, array, i);
    }
    sb.append(']');
    return sb.toString();
  }

  @Nullable
  @Override
  public ArrayT parse(@Nullable String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }
    int idx = skipSpaces(value, 0);
    idx = skipOpeningBracket(value, idx);
    idx = skipSpaces(value, idx);
    if (value.charAt(idx) == ']') {
      return newInstance(0);
    }
    // first pass: determine array length
    int length = getArrayLength(value, idx);
    // second pass: parse elements
    ArrayT array = newInstance(length);
    int i = 0;
    for (; idx < value.length(); i++) {
      int n = skipLiteral(value, idx);
      parseElement(value.substring(idx, n), array, i);
      idx = skipSpaces(value, n);
      if (value.charAt(idx) == ']') {
        return array;
      }
      idx = skipComma(value, idx);
      idx = skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format("Malformed list value \"%s\", missing closing ']'", value));
  }

  /**
   * Creates a new array instance with the given size.
   *
   * @param size The size of the array to instantiate.
   * @return a new array instance with the given size.
   */
  @NonNull
  protected abstract ArrayT newInstance(int size);

  /**
   * Formats the {@code index}th element of {@code array} to {@code output}.
   *
   * @param output The StringBuilder to write to.
   * @param array The array to read from.
   * @param index The element index.
   */
  protected abstract void formatElement(
      @NonNull StringBuilder output, @NonNull ArrayT array, int index);

  /**
   * Parses the {@code index}th element of {@code array} from {@code input}.
   *
   * @param input The String to read from.
   * @param array The array to write to.
   * @param index The element index.
   */
  protected abstract void parseElement(@NonNull String input, @NonNull ArrayT array, int index);

  private int getArrayLength(String value, int idx) {
    int length = 1;
    for (; idx < value.length(); length++) {
      idx = skipLiteral(value, idx);
      idx = skipSpaces(value, idx);
      if (value.charAt(idx) == ']') {
        break;
      }
      idx = skipComma(value, idx);
      idx = skipSpaces(value, idx);
    }
    return length;
  }

  private int skipComma(String value, int idx) {
    if (value.charAt(idx) != ',') {
      throw new IllegalArgumentException(
          String.format(
              "Cannot parse list value from \"%s\", at character %d expecting ',' but got '%c'",
              value, idx, value.charAt(idx)));
    }
    return idx + 1;
  }

  private int skipOpeningBracket(String value, int idx) {
    if (value.charAt(idx) != '[') {
      throw new IllegalArgumentException(
          String.format(
              "cannot parse list value from \"%s\", at character %d expecting '[' but got '%c'",
              value, idx, value.charAt(idx)));
    }
    return idx + 1;
  }

  private int skipSpaces(String value, int idx) {
    try {
      return ParseUtils.skipSpaces(value, idx);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot parse list value from \"%s\", at character %d expecting space but got '%c'",
              value, idx, value.charAt(idx)),
          e);
    }
  }

  private int skipLiteral(String value, int idx) {
    try {
      return ParseUtils.skipCQLValue(value, idx);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot parse list value from \"%s\", invalid CQL value at character %d", value, idx),
          e);
    }
  }
}
