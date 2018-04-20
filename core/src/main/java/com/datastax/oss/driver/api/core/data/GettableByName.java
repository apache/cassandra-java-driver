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

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveBooleanCodec;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveByteCodec;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveDoubleCodec;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveFloatCodec;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveIntCodec;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveLongCodec;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveShortCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.metadata.token.ByteOrderedToken;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
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
public interface GettableByName extends AccessibleByName {

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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  ByteBuffer getBytesUnsafe(String name);

  /**
   * Indicates whether the value for the first occurrence of {@code name} is a CQL {@code NULL}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default boolean isNull(String name) {
    return getBytesUnsafe(name) == null;
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default <T> T get(String name, TypeCodec<T> codec) {
    return codec.decode(getBytesUnsafe(name), protocolVersion());
  }

  /**
   * Returns the value for the first occurrence of {@code name}, converting it to the given Java
   * type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>This variant is for generic Java types. If the target type is not generic, use {@link
   * #get(String, Class)} instead, which may perform slightly better.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <T> T get(String name, GenericType<T> targetType) {
    DataType cqlType = getType(name);
    TypeCodec<T> codec = codecRegistry().codecFor(cqlType, targetType);
    return get(name, codec);
  }

  /**
   * Returns the value for the first occurrence of {@code name}, converting it to the given Java
   * type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>If the target type is generic, use {@link #get(String, GenericType)} instead.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <T> T get(String name, Class<T> targetClass) {
    // This is duplicated from the GenericType variant, because we want to give the codec registry
    // a chance to process the unwrapped class directly, if it can do so in a more efficient way.
    DataType cqlType = getType(name);
    TypeCodec<T> codec = codecRegistry().codecFor(cqlType, targetClass);
    return get(name, codec);
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
   * #getBoolean(String)}, {@link #getInt(String)}, etc.), or {@link #get(String, Class)} and {@link
   * #get(String, GenericType)} for custom types.
   *
   * <p>The definition of "most appropriate" is unspecified, and left to the appreciation of the
   * {@link #codecRegistry()} implementation. By default, the driver uses the mapping described in
   * the other {@code getXxx()} methods (for example {@link #getString(String) String for text,
   * varchar and ascii}, etc).
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default Object getObject(String name) {
    DataType cqlType = getType(name);
    TypeCodec<?> codec = codecRegistry().codecFor(cqlType);
    return codec.decode(getBytesUnsafe(name), protocolVersion());
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default boolean getBoolean(String name) {
    DataType cqlType = getType(name);
    TypeCodec<Boolean> codec = codecRegistry().codecFor(cqlType, Boolean.class);
    return (codec instanceof PrimitiveBooleanCodec)
        ? ((PrimitiveBooleanCodec) codec).decodePrimitive(getBytesUnsafe(name), protocolVersion())
        : get(name, codec);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default byte getByte(String name) {
    DataType cqlType = getType(name);
    TypeCodec<Byte> codec = codecRegistry().codecFor(cqlType, Byte.class);
    return (codec instanceof PrimitiveByteCodec)
        ? ((PrimitiveByteCodec) codec).decodePrimitive(getBytesUnsafe(name), protocolVersion())
        : get(name, codec);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default double getDouble(String name) {
    DataType cqlType = getType(name);
    TypeCodec<Double> codec = codecRegistry().codecFor(cqlType, Double.class);
    return (codec instanceof PrimitiveDoubleCodec)
        ? ((PrimitiveDoubleCodec) codec).decodePrimitive(getBytesUnsafe(name), protocolVersion())
        : get(name, codec);
  }

  /**
   * Returns the {@code i}th value as a Java primitive float.
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default float getFloat(String name) {
    DataType cqlType = getType(name);
    TypeCodec<Float> codec = codecRegistry().codecFor(cqlType, Float.class);
    return (codec instanceof PrimitiveFloatCodec)
        ? ((PrimitiveFloatCodec) codec).decodePrimitive(getBytesUnsafe(name), protocolVersion())
        : get(name, codec);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default int getInt(String name) {
    DataType cqlType = getType(name);
    TypeCodec<Integer> codec = codecRegistry().codecFor(cqlType, Integer.class);
    return (codec instanceof PrimitiveIntCodec)
        ? ((PrimitiveIntCodec) codec).decodePrimitive(getBytesUnsafe(name), protocolVersion())
        : get(name, codec);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default long getLong(String name) {
    DataType cqlType = getType(name);
    TypeCodec<Long> codec = codecRegistry().codecFor(cqlType, Long.class);
    return (codec instanceof PrimitiveLongCodec)
        ? ((PrimitiveLongCodec) codec).decodePrimitive(getBytesUnsafe(name), protocolVersion())
        : get(name, codec);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default short getShort(String name) {
    DataType cqlType = getType(name);
    TypeCodec<Short> codec = codecRegistry().codecFor(cqlType, Short.class);
    return (codec instanceof PrimitiveShortCodec)
        ? ((PrimitiveShortCodec) codec).decodePrimitive(getBytesUnsafe(name), protocolVersion())
        : get(name, codec);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default Instant getInstant(String name) {
    return get(name, Instant.class);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default LocalDate getLocalDate(String name) {
    return get(name, LocalDate.class);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default LocalTime getLocalTime(String name) {
    return get(name, LocalTime.class);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default ByteBuffer getByteBuffer(String name) {
    return get(name, ByteBuffer.class);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default String getString(String name) {
    return get(name, String.class);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default BigInteger getBigInteger(String name) {
    return get(name, BigInteger.class);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default BigDecimal getBigDecimal(String name) {
    return get(name, BigDecimal.class);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default UUID getUuid(String name) {
    return get(name, UUID.class);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default InetAddress getInetAddress(String name) {
    return get(name, InetAddress.class);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default CqlDuration getCqlDuration(String name) {
    return get(name, CqlDuration.class);
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
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws IllegalArgumentException if the column type can not be converted to a known token type.
   */
  default Token getToken(String name) {
    DataType type = getType(name);
    // Simply enumerate all known implementations. This goes against the concept of TokenFactory,
    // but injecting the factory here is too much of a hassle.
    // The only issue is if someone uses a custom partitioner, but this is highly unlikely, and even
    // then they can get the value manually as a workaround.
    if (type.equals(DataTypes.BIGINT)) {
      return isNull(name) ? null : new Murmur3Token(getLong(name));
    } else if (type.equals(DataTypes.BLOB)) {
      return isNull(name) ? null : new ByteOrderedToken(getByteBuffer(name));
    } else if (type.equals(DataTypes.VARINT)) {
      return isNull(name) ? null : new RandomToken(getBigInteger(name));
    } else {
      throw new IllegalArgumentException("Can't convert CQL type " + type + " into a token");
    }
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java list.
   *
   * <p>By default, this works with CQL type {@code list}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex list types, use {@link #get(String, GenericType)}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default <T> List<T> getList(String name, Class<T> elementsClass) {
    return get(name, GenericType.listOf(elementsClass));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java set.
   *
   * <p>By default, this works with CQL type {@code set}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex set types, use {@link #get(String, GenericType)}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default <T> Set<T> getSet(String name, Class<T> elementsClass) {
    return get(name, GenericType.setOf(elementsClass));
  }

  /**
   * Returns the value for the first occurrence of {@code name} as a Java map.
   *
   * <p>By default, this works with CQL type {@code map}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex map types, use {@link #get(String, GenericType)}.
   *
   * <p>If an identifier appears multiple times, this can only be used to access the first value.
   * For the other ones, use positional getters.
   *
   * <p>This method deals with case sensitivity in the way explained in the documentation of {@link
   * AccessibleByName}.
   *
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default <K, V> Map<K, V> getMap(String name, Class<K> keyClass, Class<V> valueClass) {
    return get(name, GenericType.mapOf(keyClass, valueClass));
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default UdtValue getUdtValue(String name) {
    return get(name, UdtValue.class);
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
   * @throws IndexOutOfBoundsException if the name is invalid.
   */
  default TupleValue getTupleValue(String name) {
    return get(name, TupleValue.class);
  }
}
