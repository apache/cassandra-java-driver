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

import com.datastax.oss.driver.api.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.type.codec.TypeCodec;
import com.datastax.oss.driver.api.type.reflect.GenericType;
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

/** A data structure that provides methods to retrieve its values via a name. */
public interface GettableByName extends GettableByIndex, AccessibleByName {

  /**
   * Returns the raw binary representation of the value for the first occurrence of {@code name}.
   *
   * <p>This is primarily for internal use; you'll likely want to use one of the typed getters
   * instead, to get a higher-level Java representation.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @return the raw value, or {@code null} if the CQL value is {@code NULL}. For performance
   *     reasons, this is the actual instance used internally. If you read data from the buffer,
   *     make sure to {@link ByteBuffer#duplicate() duplicate} it beforehand, or only use relative
   *     methods. If you change the buffer's index or its contents in any way, any other getter
   *     invocation for this value will have unpredictable results.
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default ByteBuffer getBytesUnsafe(String name) {
    return getBytesUnsafe(firstIndexOf(name));
  }

  /**
   * Indicates whether the value for the first occurrence of {@code name} is a CQL {@code NULL}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default boolean isNull(String name) {
    return isNull(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name}, using the given codec for the
   * conversion.
   *
   * <p>This method completely bypasses the {@link #codecRegistry()}, and forces the driver to use
   * the given codec instead. This can be useful if the codec would collide with a previously
   * registered one, or if you want to use the codec just once without registering it.
   *
   * <p>It is the caller's responsibility to ensure that the given codec is appropriate for the
   * conversion. Failing to do so will result in errors at runtime.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <T> T get(String name, TypeCodec<T> codec) {
    return get(firstIndexOf(name), codec);
  }

  /**
   * Returns the value for the first occurrence of {@code name}, converting it to the given Java
   * type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>This variant is for generic Java types. If the target type is not generic, use {@link
   * #get(int, Class)} instead, which may perform slightly better.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <T> T get(String name, GenericType<T> targetType) {
    return get(firstIndexOf(name), targetType);
  }

  /**
   * Returns the value for the first occurrence of {@code name}, converting it to the given Java
   * type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>If the target type is generic, use {@link #get(int, GenericType)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <T> T get(String name, Class<T> targetClass) {
    return get(firstIndexOf(name), targetClass);
  }

  /**
   * Returns the value for the first occurrence of {@code name}, converting it to the most
   * appropriate Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>Use this method to dynamically inspect elements when types aren't known in advance, for
   * instance if you're writing a generic row logger. If you know the target Java type, it is
   * generally preferable to use typed variants, such as the ones for built-in types ({@link
   * #getBoolean(int)}, {@link #getInt(int)}, etc.), or {@link #get(int, Class)} and {@link
   * #get(int, GenericType)} for custom types.
   *
   * <p>The definition of "most appropriate" is unspecified, and left to the appreciation of the
   * {@link #codecRegistry()} implementation. By default, the driver uses the mapping described in
   * the other {@code getXxx()} methods (for example {@link #getString(int) String for text, varchar
   * and ascii}, etc).
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default Object getObject(String name) {
    return getObject(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java primitive boolean.
   *
   * <p>By default, this works with CQL type {@code boolean}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code false}. If this doesn't work for you, either call {@link
   * #isNull(String)} before calling this method, or use {@code get(name, Boolean.class)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default boolean getBoolean(String name) {
    return getBoolean(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java primitive byte.
   *
   * <p>By default, this works with CQL type {@code tinyint}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(String)} before calling this method, or use {@code get(name, Byte.class)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default byte getByte(String name) {
    return getByte(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java primitive double.
   *
   * <p>By default, this works with CQL type {@code double}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0.0}. If this doesn't work for you, either call {@link
   * #isNull(String)} before calling this method, or use {@code get(name, Double.class)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default double getDouble(String name) {
    return getDouble(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java primitive float.
   *
   * <p>By default, this works with CQL type {@code float}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0.0}. If this doesn't work for you, either call {@link
   * #isNull(String)} before calling this method, or use {@code get(name, Float.class)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default float getFloat(String name) {
    return getFloat(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java primitive integer.
   *
   * <p>By default, this works with CQL type {@code int}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(String)} before calling this method, or use {@code get(name, Integer.class)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default int getInt(String name) {
    return getInt(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java primitive long.
   *
   * <p>By default, this works with CQL types {@code bigint} and {@code counter}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(String)} before calling this method, or use {@code get(name, Long.class)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default long getLong(String name) {
    return getLong(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java primitive short.
   *
   * <p>By default, this works with CQL type {@code smallint}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(String)} before calling this method, or use {@code get(name, Short.class)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default short getShort(String name) {
    return getShort(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java instant.
   *
   * <p>By default, this works with CQL type {@code timestamp}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default Instant getInstant(String name) {
    return getInstant(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java local date.
   *
   * <p>By default, this works with CQL type {@code date}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default LocalDate getLocalDate(String name) {
    return getLocalDate(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java local time.
   *
   * <p>By default, this works with CQL type {@code time}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default LocalTime getLocalTime(String name) {
    return getLocalTime(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java byte buffer.
   *
   * <p>By default, this works with CQL type {@code blob}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default ByteBuffer getByteBuffer(String name) {
    return getByteBuffer(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java string.
   *
   * <p>By default, this works with CQL types {@code text}, {@code varchar} and {@code ascii}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default String getString(String name) {
    return getString(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java big integer.
   *
   * <p>By default, this works with CQL type {@code varint}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default BigInteger getBigInteger(String name) {
    return getBigInteger(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java big decimal.
   *
   * <p>By default, this works with CQL type {@code decimal}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default BigDecimal getBigDecimal(String name) {
    return getBigDecimal(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java UUID.
   *
   * <p>By default, this works with CQL types {@code uuid} and {@code timeuuid}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default UUID getUuid(String name) {
    return getUuid(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java IP address.
   *
   * <p>By default, this works with CQL type {@code inet}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default InetAddress getInetAddress(String name) {
    return getInetAddress(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a duration.
   *
   * <p>By default, this works with CQL type {@code duration}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default CqlDuration getCqlDuration(String name) {
    return getCqlDuration(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java list.
   *
   * <p>By default, this works with CQL type {@code list}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex list types, use {@link #get(int, GenericType)}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <T> List<T> getList(String name, Class<T> elementsClass) {
    return getList(firstIndexOf(name), elementsClass);
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java set.
   *
   * <p>By default, this works with CQL type {@code set}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex set types, use {@link #get(int, GenericType)}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <T> Set<T> getSet(String name, Class<T> elementsClass) {
    return getSet(firstIndexOf(name), elementsClass);
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java map.
   *
   * <p>By default, this works with CQL type {@code map}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex map types, use {@link #get(int, GenericType)}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <K, V> Map<K, V> getMap(String name, Class<K> keyClass, Class<V> valueClass) {
    return getMap(firstIndexOf(name), keyClass, valueClass);
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a user defined type value.
   *
   * <p>By default, this works with CQL user-defined types.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default UdtValue getUdtValue(String name) {
    return getUdtValue(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a tuple value.
   *
   * <p>By default, this works with CQL tuples.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default TupleValue getTupleValue(String name) {
    return getTupleValue(firstIndexOf(name));
  }
}
