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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default ByteBuffer getBytesUnsafe(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  default boolean isNull(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default <ValueT> ValueT get(@NonNull String name, @NonNull TypeCodec<ValueT> codec) {
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
   * @throws IllegalArgumentException if the name is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @Nullable
  default <ValueT> ValueT get(@NonNull String name, @NonNull GenericType<ValueT> targetType) {
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
   * @throws IllegalArgumentException if the name is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @Nullable
  default <ValueT> ValueT get(@NonNull String name, @NonNull Class<ValueT> targetClass) {
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
   * @throws IllegalArgumentException if the name is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @Nullable
  default Object getObject(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  default boolean getBoolean(@NonNull String name) {
    return getBoolean(firstIndexOf(name));
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #getBoolean(String)}.
   */
  @Deprecated
  default boolean getBool(@NonNull String name) {
    return getBoolean(name);
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  default byte getByte(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  default double getDouble(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  default float getFloat(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  default int getInt(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  default long getLong(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  default short getShort(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default Instant getInstant(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default LocalDate getLocalDate(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default LocalTime getLocalTime(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default ByteBuffer getByteBuffer(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default String getString(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default BigInteger getBigInteger(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default BigDecimal getBigDecimal(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default UUID getUuid(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default InetAddress getInetAddress(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default CqlDuration getCqlDuration(@NonNull String name) {
    return getCqlDuration(firstIndexOf(name));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a vector.
   *
   * <p>By default, this works with CQL type {@code vector}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default <ElementT> CqlVector<ElementT> getVector(
      @NonNull String name, @NonNull Class<ElementT> elementsClass) {
    return getVector(firstIndexOf(name), elementsClass);
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a token.
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
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IllegalArgumentException if the column type can not be converted to a known token type
   *     or if the name is invalid.
   */
  @Nullable
  default Token getToken(@NonNull String name) {
    return getToken(firstIndexOf(name));
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
   * <p>Apache Cassandra does not make any distinction between an empty collection and {@code null}.
   * Whether this method will return an empty collection or {@code null} will depend on the codec
   * used; by default, the driver's built-in codecs all return empty collections.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default <ElementT> List<ElementT> getList(
      @NonNull String name, @NonNull Class<ElementT> elementsClass) {
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
   * <p>Apache Cassandra does not make any distinction between an empty collection and {@code null}.
   * Whether this method will return an empty collection or {@code null} will depend on the codec
   * used; by default, the driver's built-in codecs all return empty collections.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default <ElementT> Set<ElementT> getSet(
      @NonNull String name, @NonNull Class<ElementT> elementsClass) {
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
   * <p>Apache Cassandra does not make any distinction between an empty collection and {@code null}.
   * Whether this method will return an empty collection or {@code null} will depend on the codec
   * used; by default, the driver's built-in codecs all return empty collections.
   *
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default <KeyT, ValueT> Map<KeyT, ValueT> getMap(
      @NonNull String name, @NonNull Class<KeyT> keyClass, @NonNull Class<ValueT> valueClass) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default UdtValue getUdtValue(@NonNull String name) {
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
   * @throws IllegalArgumentException if the name is invalid.
   */
  @Nullable
  default TupleValue getTupleValue(@NonNull String name) {
    return getTupleValue(firstIndexOf(name));
  }
}
