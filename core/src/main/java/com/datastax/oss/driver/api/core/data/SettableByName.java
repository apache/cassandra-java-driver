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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** A data structure that provides methods to set its values via a name. */
public interface SettableByName<SelfT extends SettableByName<SelfT>>
    extends SettableByIndex<SelfT>, AccessibleByName {

  /**
   * Sets the raw binary representation of the value for all occurrences of {@code name}.
   *
   * <p>This is primarily for internal use; you'll likely want to use one of the typed setters
   * instead, to pass a higher-level Java representation.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @param v the raw value, or {@code null} to set the CQL value {@code NULL}. For performance
   *     reasons, this is the actual instance used internally. If pass in a buffer that you're going
   *     to modify elsewhere in your application, make sure to {@link ByteBuffer#duplicate()
   *     duplicate} it beforehand. If you change the buffer's index or its contents in any way,
   *     further usage of this data will have unpredictable results.
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setBytesUnsafe(@NonNull String name, @Nullable ByteBuffer v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setBytesUnsafe(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  @NonNull
  @Override
  default DataType getType(@NonNull String name) {
    return getType(firstIndexOf(name));
  }

  /**
   * Sets the value for all occurrences of {@code name} to CQL {@code NULL}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setToNull(@NonNull String name) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setToNull(i);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name}, using the given codec for the conversion.
   *
   * <p>This method completely bypasses the {@link #codecRegistry()}, and forces the driver to use
   * the given codec instead. This can be useful if the codec would collide with a previously
   * registered one, or if you want to use the codec just once without registering it.
   *
   * <p>It is the caller's responsibility to ensure that the given codec is appropriate for the
   * conversion. Failing to do so will result in errors at runtime.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default <ValueT> SelfT set(
      @NonNull String name, @Nullable ValueT v, @NonNull TypeCodec<ValueT> codec) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).set(i, v, codec);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name}, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>This variant is for generic Java types. If the target type is not generic, use {@link
   * #set(int, Object, Class)} instead, which may perform slightly better.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @NonNull
  @CheckReturnValue
  default <ValueT> SelfT set(
      @NonNull String name, @Nullable ValueT v, @NonNull GenericType<ValueT> targetType) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).set(i, v, targetType);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Returns the value for all occurrences of {@code name}, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>If the target type is generic, use {@link #set(int, Object, GenericType)} instead.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @NonNull
  @CheckReturnValue
  default <ValueT> SelfT set(
      @NonNull String name, @Nullable ValueT v, @NonNull Class<ValueT> targetClass) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).set(i, v, targetClass);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java primitive boolean.
   *
   * <p>By default, this works with CQL type {@code boolean}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Boolean.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setBoolean(@NonNull String name, boolean v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setBoolean(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias
   *     for{@link #setBoolean(String, boolean)}.
   */
  @Deprecated
  @NonNull
  @CheckReturnValue
  default SelfT setBool(@NonNull String name, boolean v) {
    return setBoolean(name, v);
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java primitive byte.
   *
   * <p>By default, this works with CQL type {@code tinyint}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Boolean.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setByte(@NonNull String name, byte v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setByte(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java primitive double.
   *
   * <p>By default, this works with CQL type {@code double}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Double.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setDouble(@NonNull String name, double v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setDouble(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java primitive float.
   *
   * <p>By default, this works with CQL type {@code float}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Float.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setFloat(@NonNull String name, float v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setFloat(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java primitive integer.
   *
   * <p>By default, this works with CQL type {@code int}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Integer.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setInt(@NonNull String name, int v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setInt(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java primitive long.
   *
   * <p>By default, this works with CQL types {@code bigint} and {@code counter}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Long.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setLong(@NonNull String name, long v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setLong(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java primitive short.
   *
   * <p>By default, this works with CQL type {@code smallint}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Short.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setShort(@NonNull String name, short v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setShort(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java instant.
   *
   * <p>By default, this works with CQL type {@code timestamp}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setInstant(@NonNull String name, @Nullable Instant v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setInstant(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java local date.
   *
   * <p>By default, this works with CQL type {@code date}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setLocalDate(@NonNull String name, @Nullable LocalDate v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setLocalDate(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java local time.
   *
   * <p>By default, this works with CQL type {@code time}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setLocalTime(@NonNull String name, @Nullable LocalTime v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setLocalTime(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java byte buffer.
   *
   * <p>By default, this works with CQL type {@code blob}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setByteBuffer(@NonNull String name, @Nullable ByteBuffer v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setByteBuffer(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java string.
   *
   * <p>By default, this works with CQL types {@code text}, {@code varchar} and {@code ascii}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setString(@NonNull String name, @Nullable String v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setString(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java big integer.
   *
   * <p>By default, this works with CQL type {@code varint}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setBigInteger(@NonNull String name, @Nullable BigInteger v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setBigInteger(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java big decimal.
   *
   * <p>By default, this works with CQL type {@code decimal}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setBigDecimal(@NonNull String name, @Nullable BigDecimal v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setBigDecimal(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java UUID.
   *
   * <p>By default, this works with CQL types {@code uuid} and {@code timeuuid}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setUuid(@NonNull String name, @Nullable UUID v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setUuid(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java IP address.
   *
   * <p>By default, this works with CQL type {@code inet}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setInetAddress(@NonNull String name, @Nullable InetAddress v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setInetAddress(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided duration.
   *
   * <p>By default, this works with CQL type {@code duration}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setCqlDuration(@NonNull String name, @Nullable CqlDuration v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setCqlDuration(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided vector.
   *
   * <p>By default, this works with CQL type {@code vector}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default <ElementT> SelfT setVector(
      @NonNull String name,
      @Nullable CqlVector<ElementT> v,
      @NonNull Class<ElementT> elementsClass) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setVector(i, v, elementsClass);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided token.
   *
   * <p>This works with the CQL type matching the partitioner in use for this cluster: {@code
   * bigint} for {@code Murmur3Partitioner}, {@code blob} for {@code ByteOrderedPartitioner}, and
   * {@code varint} for {@code RandomPartitioner}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setToken(@NonNull String name, @NonNull Token v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setToken(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java list.
   *
   * <p>By default, this works with CQL type {@code list}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex list types, use {@link #set(int, Object, GenericType)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default <ElementT> SelfT setList(
      @NonNull String name, @Nullable List<ElementT> v, @NonNull Class<ElementT> elementsClass) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setList(i, v, elementsClass);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java set.
   *
   * <p>By default, this works with CQL type {@code set}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex set types, use {@link #set(int, Object, GenericType)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default <ElementT> SelfT setSet(
      @NonNull String name, @Nullable Set<ElementT> v, @NonNull Class<ElementT> elementsClass) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setSet(i, v, elementsClass);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided Java map.
   *
   * <p>By default, this works with CQL type {@code map}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex map types, use {@link #set(int, Object, GenericType)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default <KeyT, ValueT> SelfT setMap(
      @NonNull String name,
      @Nullable Map<KeyT, ValueT> v,
      @NonNull Class<KeyT> keyClass,
      @NonNull Class<ValueT> valueClass) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setMap(i, v, keyClass, valueClass);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided user defined type value.
   *
   * <p>By default, this works with CQL user-defined types.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setUdtValue(@NonNull String name, @Nullable UdtValue v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setUdtValue(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }

  /**
   * Sets the value for all occurrences of {@code name} to the provided tuple value.
   *
   * <p>By default, this works with CQL tuples.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @NonNull
  @CheckReturnValue
  default SelfT setTupleValue(@NonNull String name, @Nullable TupleValue v) {
    SelfT result = null;
    for (Integer i : allIndicesOf(name)) {
      result = (result == null ? this : result).setTupleValue(i, v);
    }
    assert result != null; // allIndices throws if there are no results
    return result;
  }
}
