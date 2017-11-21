/*
 * Copyright DataStax, Inc.
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
import com.datastax.oss.driver.api.core.metadata.token.Token;
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

/** A data structure that provides methods to retrieve its values via a CQL identifier. */
public interface GettableById extends GettableByIndex, AccessibleById {

  /**
   * Returns the raw binary representation of the value for the first occurrence of {@code id}.
   *
   * <p>This is primarily for internal use; you'll likely want to use one of the typed getters
   * instead, to get a higher-level Java representation.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @return the raw value, or {@code null} if the CQL value is {@code NULL}. For performance
   *     reasons, this is the actual instance used internally. If you read data from the buffer,
   *     make sure to {@link ByteBuffer#duplicate() duplicate} it beforehand, or only use relative
   *     methods. If you change the buffer's index or its contents in any way, any other getter
   *     invocation for this value will have unpredictable results.
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default ByteBuffer getBytesUnsafe(CqlIdentifier id) {
    return getBytesUnsafe(firstIndexOf(id));
  }

  /**
   * Indicates whether the value for the first occurrence of {@code id} is a CQL {@code NULL}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default boolean isNull(CqlIdentifier id) {
    return isNull(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id}, using the given codec for the
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
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default <T> T get(CqlIdentifier id, TypeCodec<T> codec) {
    return get(firstIndexOf(id), codec);
  }

  /**
   * Returns the value for the first occurrence of {@code id}, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>This variant is for generic Java types. If the target type is not generic, use {@link
   * #get(int, Class)} instead, which may perform slightly better.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <T> T get(CqlIdentifier id, GenericType<T> targetType) {
    return get(firstIndexOf(id), targetType);
  }

  /**
   * Returns the value for the first occurrence of {@code id}, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>If the target type is generic, use {@link #get(int, GenericType)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <T> T get(CqlIdentifier id, Class<T> targetClass) {
    return get(firstIndexOf(id), targetClass);
  }

  /**
   * Returns the value for the first occurrence of {@code id}, converting it to the most appropriate
   * Java type.
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
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default Object getObject(CqlIdentifier id) {
    return getObject(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java primitive boolean.
   *
   * <p>By default, this works with CQL type {@code boolean}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code false}. If this doesn't work for you, either call {@link
   * #isNull(CqlIdentifier)} before calling this method, or use {@code get(id, Boolean.class)}
   * instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default boolean getBoolean(CqlIdentifier id) {
    return getBoolean(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java primitive byte.
   *
   * <p>By default, this works with CQL type {@code tinyint}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(CqlIdentifier)} before calling this method, or use {@code get(id, Byte.class)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default byte getByte(CqlIdentifier id) {
    return getByte(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java primitive double.
   *
   * <p>By default, this works with CQL type {@code double}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0.0}. If this doesn't work for you, either call {@link
   * #isNull(CqlIdentifier)} before calling this method, or use {@code get(id, Double.class)}
   * instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default double getDouble(CqlIdentifier id) {
    return getDouble(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java primitive float.
   *
   * <p>By default, this works with CQL type {@code float}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0.0}. If this doesn't work for you, either call {@link
   * #isNull(CqlIdentifier)} before calling this method, or use {@code get(id, Float.class)}
   * instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default float getFloat(CqlIdentifier id) {
    return getFloat(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java primitive integer.
   *
   * <p>By default, this works with CQL type {@code int}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(CqlIdentifier)} before calling this method, or use {@code get(id, Integer.class)}
   * instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default int getInt(CqlIdentifier id) {
    return getInt(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java primitive long.
   *
   * <p>By default, this works with CQL types {@code bigint} and {@code counter}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(CqlIdentifier)} before calling this method, or use {@code get(id, Long.class)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default long getLong(CqlIdentifier id) {
    return getLong(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java primitive short.
   *
   * <p>By default, this works with CQL type {@code smallint}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(CqlIdentifier)} before calling this method, or use {@code get(id, Short.class)}
   * instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default short getShort(CqlIdentifier id) {
    return getShort(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java instant.
   *
   * <p>By default, this works with CQL type {@code timestamp}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default Instant getInstant(CqlIdentifier id) {
    return getInstant(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java local date.
   *
   * <p>By default, this works with CQL type {@code date}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default LocalDate getLocalDate(CqlIdentifier id) {
    return getLocalDate(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java local time.
   *
   * <p>By default, this works with CQL type {@code time}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default LocalTime getLocalTime(CqlIdentifier id) {
    return getLocalTime(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java byte buffer.
   *
   * <p>By default, this works with CQL type {@code blob}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default ByteBuffer getByteBuffer(CqlIdentifier id) {
    return getByteBuffer(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java string.
   *
   * <p>By default, this works with CQL types {@code text}, {@code varchar} and {@code ascii}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default String getString(CqlIdentifier id) {
    return getString(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java big integer.
   *
   * <p>By default, this works with CQL type {@code varint}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default BigInteger getBigInteger(CqlIdentifier id) {
    return getBigInteger(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java big decimal.
   *
   * <p>By default, this works with CQL type {@code decimal}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default BigDecimal getBigDecimal(CqlIdentifier id) {
    return getBigDecimal(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java UUID.
   *
   * <p>By default, this works with CQL types {@code uuid} and {@code timeuuid}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default UUID getUuid(CqlIdentifier id) {
    return getUuid(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java IP address.
   *
   * <p>By default, this works with CQL type {@code inet}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default InetAddress getInetAddress(CqlIdentifier id) {
    return getInetAddress(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a duration.
   *
   * <p>By default, this works with CQL type {@code duration}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default CqlDuration getCqlDuration(CqlIdentifier id) {
    return getCqlDuration(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a token.
   *
   * <p>Note that, for simplicity, this method relies on the CQL type of the column to pick the
   * correct token implementation. Therefore it must only be called on columns of the type that
   * matches the partitioner in use for this cluster: {@code bigint} for {@code Murmur3Partitioner},
   * {@code blob} for {@code ByteOrderedPartitioner}, and {@code varint} for {@code
   * RandomPartitioner}. Calling it for the wrong type will produce corrupt tokens that are unusable
   * with this driver instance.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws IllegalArgumentException if the column type can not be converted to a known token type.
   */
  default Token getToken(CqlIdentifier id) {
    return getToken(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java list.
   *
   * <p>By default, this works with CQL type {@code list}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex list types, use {@link #get(int, GenericType)}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default <T> List<T> getList(CqlIdentifier id, Class<T> elementsClass) {
    return getList(firstIndexOf(id), elementsClass);
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java set.
   *
   * <p>By default, this works with CQL type {@code set}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex set types, use {@link #get(int, GenericType)}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default <T> Set<T> getSet(CqlIdentifier id, Class<T> elementsClass) {
    return getSet(firstIndexOf(id), elementsClass);
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a Java map.
   *
   * <p>By default, this works with CQL type {@code map}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex map types, use {@link #get(int, GenericType)}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default <K, V> Map<K, V> getMap(CqlIdentifier id, Class<K> keyClass, Class<V> valueClass) {
    return getMap(firstIndexOf(id), keyClass, valueClass);
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a user defined type value.
   *
   * <p>By default, this works with CQL user-defined types.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default UdtValue getUdtValue(CqlIdentifier id) {
    return getUdtValue(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a tuple value.
   *
   * <p>By default, this works with CQL tuples.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IndexOutOfBoundsException if the id is invalid.
   */
  default TupleValue getTupleValue(CqlIdentifier id) {
    return getTupleValue(firstIndexOf(id));
  }
}
