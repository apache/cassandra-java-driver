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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default ByteBuffer getBytesUnsafe(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  default boolean isNull(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default <ValueT> ValueT get(@NonNull CqlIdentifier id, @NonNull TypeCodec<ValueT> codec) {
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
   * @throws IllegalArgumentException if the id is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @Nullable
  default <ValueT> ValueT get(@NonNull CqlIdentifier id, @NonNull GenericType<ValueT> targetType) {
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
   * @throws IllegalArgumentException if the id is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @Nullable
  default <ValueT> ValueT get(@NonNull CqlIdentifier id, @NonNull Class<ValueT> targetClass) {
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
   * @throws IllegalArgumentException if the id is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @Nullable
  default Object getObject(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  default boolean getBoolean(@NonNull CqlIdentifier id) {
    return getBoolean(firstIndexOf(id));
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #getBoolean(CqlIdentifier)}.
   */
  @Deprecated
  default boolean getBool(@NonNull CqlIdentifier id) {
    return getBoolean(id);
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  default byte getByte(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  default double getDouble(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  default float getFloat(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  default int getInt(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  default long getLong(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  default short getShort(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default Instant getInstant(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default LocalDate getLocalDate(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default LocalTime getLocalTime(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default ByteBuffer getByteBuffer(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default String getString(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default BigInteger getBigInteger(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default BigDecimal getBigDecimal(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default UUID getUuid(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default InetAddress getInetAddress(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default CqlDuration getCqlDuration(@NonNull CqlIdentifier id) {
    return getCqlDuration(firstIndexOf(id));
  }

  /**
   * Returns the value for the first occurrence of {@code id} as a vector.
   *
   * <p>By default, this works with CQL type {@code vector}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>If you want to avoid the overhead of building a {@code CqlIdentifier}, use the variant of
   * this method that takes a string argument.
   *
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default <ElementT extends Number> CqlVector<ElementT> getVector(
      @NonNull CqlIdentifier id, @NonNull Class<ElementT> elementsClass) {
    return getVector(firstIndexOf(id), elementsClass);
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
   * @throws IllegalArgumentException if the column type can not be converted to a known token type
   *     or if the name is invalid.
   */
  @Nullable
  default Token getToken(@NonNull CqlIdentifier id) {
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
   * <p>Apache Cassandra does not make any distinction between an empty collection and {@code null}.
   * Whether this method will return an empty collection or {@code null} will depend on the codec
   * used; by default, the driver's built-in codecs all return empty collections.
   *
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default <ElementT> List<ElementT> getList(
      @NonNull CqlIdentifier id, @NonNull Class<ElementT> elementsClass) {
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
   * <p>Apache Cassandra does not make any distinction between an empty collection and {@code null}.
   * Whether this method will return an empty collection or {@code null} will depend on the codec
   * used; by default, the driver's built-in codecs all return empty collections.
   *
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default <ElementT> Set<ElementT> getSet(
      @NonNull CqlIdentifier id, @NonNull Class<ElementT> elementsClass) {
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
   * <p>Apache Cassandra does not make any distinction between an empty collection and {@code null}.
   * Whether this method will return an empty collection or {@code null} will depend on the codec
   * used; by default, the driver's built-in codecs all return empty collections.
   *
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default <KeyT, ValueT> Map<KeyT, ValueT> getMap(
      @NonNull CqlIdentifier id, @NonNull Class<KeyT> keyClass, @NonNull Class<ValueT> valueClass) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default UdtValue getUdtValue(@NonNull CqlIdentifier id) {
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
   * @throws IllegalArgumentException if the id is invalid.
   */
  @Nullable
  default TupleValue getTupleValue(@NonNull CqlIdentifier id) {
    return getTupleValue(firstIndexOf(id));
  }
}
