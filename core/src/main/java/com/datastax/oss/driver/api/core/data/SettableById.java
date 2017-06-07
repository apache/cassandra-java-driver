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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
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

/** A data structure that provides methods to set its values via a CQL identifier. */
public interface SettableById<T extends SettableById<T>>
    extends SettableByIndex<T>, AccessibleById {

  /**
   * Sets the raw binary representation of the value for the first occurrence of {@code id}.
   *
   * <p>This is primarily for internal use; you'll likely want to use one of the typed setters
   * instead, to pass a higher-level Java representation.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @param v the raw value, or {@code null} to set the CQL value {@code NULL}. For performance
   *     reasons, this is the actual instance used internally. If pass in a buffer that you're going
   *     to modify elsewhere in your application, make sure to {@link ByteBuffer#duplicate()
   *     duplicate} it beforehand. If you change the buffer's index or its contents in any way,
   *     further usage of this data will have unpredictable results.
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setBytesUnsafe(CqlIdentifier id, ByteBuffer v) {
    return setBytesUnsafe(firstIndexOf(id), v);
  }

  @Override
  default DataType getType(CqlIdentifier id) {
    return getType(firstIndexOf(id));
  }

  /**
   * Sets the value for the first occurrence of {@code id} to CQL {@code NULL}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setToNull(CqlIdentifier id) {
    return setToNull(firstIndexOf(id));
  }

  /**
   * Sets the value for the first occurrence of {@code id}, using the given codec for the
   * conversion.
   *
   * <p>This method completely bypasses the {@link #codecRegistry()}, and forces the driver to use
   * the given codec instead. This can be useful if the codec would collide with a previously
   * registered one, or if you want to use the codec just once without registering it.
   *
   * <p>It is the caller's responsibility to ensure that the given codec is appropriate for the
   * conversion. Failing to do so will result in errors at runtime.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <V> T set(CqlIdentifier id, V v, TypeCodec<V> codec) {
    return set(firstIndexOf(id), v, codec);
  }

  /**
   * Sets the value for the first occurrence of {@code id}, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>This variant is for generic Java types. If the target type is not generic, use {@link
   * #set(int, V, Class)} instead, which may perform slightly better.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <V> T set(CqlIdentifier id, V v, GenericType<V> targetType) {
    return set(firstIndexOf(id), v, targetType);
  }

  /**
   * Returns the value for the first occurrence of {@code id}, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>If the target type is generic, use {@link #set(int, Object, GenericType)} instead.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <V> T set(CqlIdentifier id, V v, Class<V> targetClass) {
    return set(firstIndexOf(id), v, targetClass);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java primitive boolean.
   *
   * <p>By default, this works with CQL type {@code boolean}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Boolean.class)}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setBoolean(CqlIdentifier id, boolean v) {
    return setBoolean(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java primitive byte.
   *
   * <p>By default, this works with CQL type {@code tinyint}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Boolean.class)}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setByte(CqlIdentifier id, byte v) {
    return setByte(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java primitive double.
   *
   * <p>By default, this works with CQL type {@code double}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Double.class)}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setDouble(CqlIdentifier id, double v) {
    return setDouble(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java primitive float.
   *
   * <p>By default, this works with CQL type {@code float}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Float.class)}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setFloat(CqlIdentifier id, float v) {
    return setFloat(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java primitive integer.
   *
   * <p>By default, this works with CQL type {@code int}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Integer.class)}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setInt(CqlIdentifier id, int v) {
    return setInt(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java primitive long.
   *
   * <p>By default, this works with CQL types {@code bigint} and {@code counter}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Long.class)}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setLong(CqlIdentifier id, long v) {
    return setLong(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java primitive short.
   *
   * <p>By default, this works with CQL type {@code smallint}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Short.class)}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setShort(CqlIdentifier id, short v) {
    return setShort(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java instant.
   *
   * <p>By default, this works with CQL type {@code timestamp}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setInstant(CqlIdentifier id, Instant v) {
    return setInstant(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java local date.
   *
   * <p>By default, this works with CQL type {@code date}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setLocalDate(CqlIdentifier id, LocalDate v) {
    return setLocalDate(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java local time.
   *
   * <p>By default, this works with CQL type {@code time}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setLocalTime(CqlIdentifier id, LocalTime v) {
    return setLocalTime(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java byte buffer.
   *
   * <p>By default, this works with CQL type {@code blob}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setByteBuffer(CqlIdentifier id, ByteBuffer v) {
    return setByteBuffer(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java string.
   *
   * <p>By default, this works with CQL types {@code text}, {@code varchar} and {@code ascii}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setString(CqlIdentifier id, String v) {
    return setString(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java big integer.
   *
   * <p>By default, this works with CQL type {@code varint}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setBigInteger(CqlIdentifier id, BigInteger v) {
    return setBigInteger(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java big decimal.
   *
   * <p>By default, this works with CQL type {@code decimal}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setBigDecimal(CqlIdentifier id, BigDecimal v) {
    return setBigDecimal(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java UUID.
   *
   * <p>By default, this works with CQL types {@code uuid} and {@code timeuuid}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setUuid(CqlIdentifier id, UUID v) {
    return setUuid(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java IP address.
   *
   * <p>By default, this works with CQL type {@code inet}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setInetAddress(CqlIdentifier id, InetAddress v) {
    return setInetAddress(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided duration.
   *
   * <p>By default, this works with CQL type {@code duration}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setCqlDuration(CqlIdentifier id, CqlDuration v) {
    return setCqlDuration(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java list.
   *
   * <p>By default, this works with CQL type {@code list}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex list types, use {@link #set(int, Object, GenericType)}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <V> T setList(CqlIdentifier id, List<V> v, Class<V> elementsClass) {
    return setList(firstIndexOf(id), v, elementsClass);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java set.
   *
   * <p>By default, this works with CQL type {@code set}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex set types, use {@link #set(int, Object, GenericType)}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <V> T setSet(CqlIdentifier id, Set<V> v, Class<V> elementsClass) {
    return setSet(firstIndexOf(id), v, elementsClass);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided Java map.
   *
   * <p>By default, this works with CQL type {@code map}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex map types, use {@link #set(int, Object, GenericType)}.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <K, V> T setMap(CqlIdentifier id, Map<K, V> v, Class<K> keyClass, Class<V> valueClass) {
    return setMap(firstIndexOf(id), v, keyClass, valueClass);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided user defined type value.
   *
   * <p>By default, this works with CQL user-defined types.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setUdtValue(CqlIdentifier id, UdtValue v) {
    return setUdtValue(firstIndexOf(id), v);
  }

  /**
   * Sets the value for the first occurrence of {@code id} to the provided tuple value.
   *
   * <p>By default, this works with CQL tuples.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setTupleValue(CqlIdentifier id, TupleValue v) {
    return setTupleValue(firstIndexOf(id), v);
  }
}
