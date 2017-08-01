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

/** A data structure that provides methods to set its values via a name. */
public interface SettableByName<T extends SettableByName<T>>
    extends SettableByIndex<T>, AccessibleByName {

  /**
   * Sets the raw binary representation of the value for the first occurrence of {@code name}.
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setBytesUnsafe(String name, ByteBuffer v) {
    return setBytesUnsafe(firstIndexOf(name), v);
  }

  @Override
  default DataType getType(String name) {
    return getType(firstIndexOf(name));
  }

  /**
   * Sets the value for the first occurrence of {@code name} to CQL {@code NULL}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setToNull(String name) {
    return setToNull(firstIndexOf(name));
  }

  /**
   * Sets the value for the first occurrence of {@code name}, using the given codec for the
   * conversion.
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default <V> T set(String name, V v, TypeCodec<V> codec) {
    return set(firstIndexOf(name), v, codec);
  }

  /**
   * Sets the value for the first occurrence of {@code name}, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>This variant is for generic Java types. If the target type is not generic, use {@link
   * #set(int, V, Class)} instead, which may perform slightly better.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <V> T set(String name, V v, GenericType<V> targetType) {
    return set(firstIndexOf(name), v, targetType);
  }

  /**
   * Returns the value for the first occurrence of {@code name}, converting it to the given Java
   * type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>If the target type is generic, use {@link #set(int, Object, GenericType)} instead.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <V> T set(String name, V v, Class<V> targetClass) {
    return set(firstIndexOf(name), v, targetClass);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java primitive boolean.
   *
   * <p>By default, this works with CQL type {@code boolean}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Boolean.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setBoolean(String name, boolean v) {
    return setBoolean(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java primitive byte.
   *
   * <p>By default, this works with CQL type {@code tinyint}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Boolean.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setByte(String name, byte v) {
    return setByte(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java primitive double.
   *
   * <p>By default, this works with CQL type {@code double}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Double.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setDouble(String name, double v) {
    return setDouble(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java primitive float.
   *
   * <p>By default, this works with CQL type {@code float}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Float.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setFloat(String name, float v) {
    return setFloat(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java primitive integer.
   *
   * <p>By default, this works with CQL type {@code int}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Integer.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setInt(String name, int v) {
    return setInt(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java primitive long.
   *
   * <p>By default, this works with CQL types {@code bigint} and {@code counter}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Long.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setLong(String name, long v) {
    return setLong(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java primitive short.
   *
   * <p>By default, this works with CQL type {@code smallint}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Short.class)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setShort(String name, short v) {
    return setShort(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java instant.
   *
   * <p>By default, this works with CQL type {@code timestamp}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setInstant(String name, Instant v) {
    return setInstant(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java local date.
   *
   * <p>By default, this works with CQL type {@code date}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setLocalDate(String name, LocalDate v) {
    return setLocalDate(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java local time.
   *
   * <p>By default, this works with CQL type {@code time}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setLocalTime(String name, LocalTime v) {
    return setLocalTime(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java byte buffer.
   *
   * <p>By default, this works with CQL type {@code blob}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setByteBuffer(String name, ByteBuffer v) {
    return setByteBuffer(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java string.
   *
   * <p>By default, this works with CQL types {@code text}, {@code varchar} and {@code ascii}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setString(String name, String v) {
    return setString(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java big integer.
   *
   * <p>By default, this works with CQL type {@code varint}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setBigInteger(String name, BigInteger v) {
    return setBigInteger(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java big decimal.
   *
   * <p>By default, this works with CQL type {@code decimal}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setBigDecimal(String name, BigDecimal v) {
    return setBigDecimal(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java UUID.
   *
   * <p>By default, this works with CQL types {@code uuid} and {@code timeuuid}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setUuid(String name, UUID v) {
    return setUuid(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java IP address.
   *
   * <p>By default, this works with CQL type {@code inet}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setInetAddress(String name, InetAddress v) {
    return setInetAddress(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided duration.
   *
   * <p>By default, this works with CQL type {@code duration}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setCqlDuration(String name, CqlDuration v) {
    return setCqlDuration(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java list.
   *
   * <p>By default, this works with CQL type {@code list}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex list types, use {@link #set(int, Object, GenericType)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default <V> T setList(String name, List<V> v, Class<V> elementsClass) {
    return setList(firstIndexOf(name), v, elementsClass);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java set.
   *
   * <p>By default, this works with CQL type {@code set}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex set types, use {@link #set(int, Object, GenericType)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default <V> T setSet(String name, Set<V> v, Class<V> elementsClass) {
    return setSet(firstIndexOf(name), v, elementsClass);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided Java map.
   *
   * <p>By default, this works with CQL type {@code map}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex map types, use {@link #set(int, Object, GenericType)}.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default <K, V> T setMap(String name, Map<K, V> v, Class<K> keyClass, Class<V> valueClass) {
    return setMap(firstIndexOf(name), v, keyClass, valueClass);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided user defined type
   * value.
   *
   * <p>By default, this works with CQL user-defined types.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setUdtValue(String name, UdtValue v) {
    return setUdtValue(firstIndexOf(name), v);
  }

  /**
   * Sets the value for the first occurrence of {@code name} to the provided tuple value.
   *
   * <p>By default, this works with CQL tuples.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default T setTupleValue(String name, TupleValue v) {
    return setTupleValue(firstIndexOf(name), v);
  }
}
