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

/** A data structure that provides methods to retrieve its values via an integer index. */
public interface GettableByIndex extends AccessibleByIndex {

  /**
   * Returns the raw binary representation of the {@code i}th value.
   *
   * <p>This is primarily for internal use; you'll likely want to use one of the typed getters
   * instead, to get a higher-level Java representation.
   *
   * @return the raw value, or {@code null} if the CQL value is {@code NULL}. For performance
   *     reasons, this is the actual instance used internally. If you read data from the buffer,
   *     make sure to {@link ByteBuffer#duplicate() duplicate} it beforehand, or only use relative
   *     methods. If you change the buffer's index or its contents in any way, any other getter
   *     invocation for this value will have unpredictable results.
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  ByteBuffer getBytesUnsafe(int i);

  /**
   * Indicates whether the {@code i}th value is a CQL {@code NULL}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default boolean isNull(int i) {
    return getBytesUnsafe(i) == null;
  }

  /**
   * Returns the {@code i}th value, using the given codec for the conversion.
   *
   * <p>This method completely bypasses the {@link #codecRegistry()}, and forces the driver to use
   * the given codec instead. This can be useful if the codec would collide with a previously
   * registered one, or if you want to use the codec just once without registering it.
   *
   * <p>It is the caller's responsibility to ensure that the given codec is appropriate for the
   * conversion. Failing to do so will result in errors at runtime.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default <ValueT> ValueT get(int i, TypeCodec<ValueT> codec) {
    return codec.decode(getBytesUnsafe(i), protocolVersion());
  }

  /**
   * Returns the {@code i}th value, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>This variant is for generic Java types. If the target type is not generic, use {@link
   * #get(int, Class)} instead, which may perform slightly better.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @Nullable
  default <ValueT> ValueT get(int i, GenericType<ValueT> targetType) {
    DataType cqlType = getType(i);
    TypeCodec<ValueT> codec = codecRegistry().codecFor(cqlType, targetType);
    return get(i, codec);
  }

  /**
   * Returns the {@code i}th value, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>If the target type is generic, use {@link #get(int, GenericType)} instead.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @Nullable
  default <ValueT> ValueT get(int i, Class<ValueT> targetClass) {
    // This is duplicated from the GenericType variant, because we want to give the codec registry
    // a chance to process the unwrapped class directly, if it can do so in a more efficient way.
    DataType cqlType = getType(i);
    TypeCodec<ValueT> codec = codecRegistry().codecFor(cqlType, targetClass);
    return get(i, codec);
  }

  /**
   * Returns the {@code i}th value, converting it to the most appropriate Java type.
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
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  @Nullable
  default Object getObject(int i) {
    DataType cqlType = getType(i);
    TypeCodec<?> codec = codecRegistry().codecFor(cqlType);
    return codec.decode(getBytesUnsafe(i), protocolVersion());
  }

  /**
   * Returns the {@code i}th value as a Java primitive boolean.
   *
   * <p>By default, this works with CQL type {@code boolean}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code false}. If this doesn't work for you, either call {@link
   * #isNull(int)} before calling this method, or use {@code get(i, Boolean.class)} instead.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default boolean getBoolean(int i) {
    DataType cqlType = getType(i);
    TypeCodec<Boolean> codec = codecRegistry().codecFor(cqlType, Boolean.class);
    if (codec instanceof PrimitiveBooleanCodec) {
      return ((PrimitiveBooleanCodec) codec).decodePrimitive(getBytesUnsafe(i), protocolVersion());
    } else {
      Boolean value = get(i, codec);
      return value == null ? false : value;
    }
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #getBoolean(int)}.
   */
  @Deprecated
  default boolean getBool(int i) {
    return getBoolean(i);
  }

  /**
   * Returns the {@code i}th value as a Java primitive byte.
   *
   * <p>By default, this works with CQL type {@code tinyint}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(int)} before calling this method, or use {@code get(i, Byte.class)} instead.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default byte getByte(int i) {
    DataType cqlType = getType(i);
    TypeCodec<Byte> codec = codecRegistry().codecFor(cqlType, Byte.class);
    if (codec instanceof PrimitiveByteCodec) {
      return ((PrimitiveByteCodec) codec).decodePrimitive(getBytesUnsafe(i), protocolVersion());
    } else {
      Byte value = get(i, codec);
      return value == null ? 0 : value;
    }
  }

  /**
   * Returns the {@code i}th value as a Java primitive double.
   *
   * <p>By default, this works with CQL type {@code double}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0.0}. If this doesn't work for you, either call {@link
   * #isNull(int)} before calling this method, or use {@code get(i, Double.class)} instead.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default double getDouble(int i) {
    DataType cqlType = getType(i);
    TypeCodec<Double> codec = codecRegistry().codecFor(cqlType, Double.class);
    if (codec instanceof PrimitiveDoubleCodec) {
      return ((PrimitiveDoubleCodec) codec).decodePrimitive(getBytesUnsafe(i), protocolVersion());
    } else {
      Double value = get(i, codec);
      return value == null ? 0 : value;
    }
  }

  /**
   * Returns the {@code i}th value as a Java primitive float.
   *
   * <p>By default, this works with CQL type {@code float}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0.0}. If this doesn't work for you, either call {@link
   * #isNull(int)} before calling this method, or use {@code get(i, Float.class)} instead.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default float getFloat(int i) {
    DataType cqlType = getType(i);
    TypeCodec<Float> codec = codecRegistry().codecFor(cqlType, Float.class);
    if (codec instanceof PrimitiveFloatCodec) {
      return ((PrimitiveFloatCodec) codec).decodePrimitive(getBytesUnsafe(i), protocolVersion());
    } else {
      Float value = get(i, codec);
      return value == null ? 0 : value;
    }
  }

  /**
   * Returns the {@code i}th value as a Java primitive integer.
   *
   * <p>By default, this works with CQL type {@code int}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(int)} before calling this method, or use {@code get(i, Integer.class)} instead.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default int getInt(int i) {
    DataType cqlType = getType(i);
    TypeCodec<Integer> codec = codecRegistry().codecFor(cqlType, Integer.class);
    if (codec instanceof PrimitiveIntCodec) {
      return ((PrimitiveIntCodec) codec).decodePrimitive(getBytesUnsafe(i), protocolVersion());
    } else {
      Integer value = get(i, codec);
      return value == null ? 0 : value;
    }
  }

  /**
   * Returns the {@code i}th value as a Java primitive long.
   *
   * <p>By default, this works with CQL types {@code bigint} and {@code counter}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(int)} before calling this method, or use {@code get(i, Long.class)} instead.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default long getLong(int i) {
    DataType cqlType = getType(i);
    TypeCodec<Long> codec = codecRegistry().codecFor(cqlType, Long.class);
    if (codec instanceof PrimitiveLongCodec) {
      return ((PrimitiveLongCodec) codec).decodePrimitive(getBytesUnsafe(i), protocolVersion());
    } else {
      Long value = get(i, codec);
      return value == null ? 0 : value;
    }
  }

  /**
   * Returns the {@code i}th value as a Java primitive short.
   *
   * <p>By default, this works with CQL type {@code smallint}.
   *
   * <p>Note that, due to its signature, this method cannot return {@code null}. If the CQL value is
   * {@code NULL}, it will return {@code 0}. If this doesn't work for you, either call {@link
   * #isNull(int)} before calling this method, or use {@code get(i, Short.class)} instead.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default short getShort(int i) {
    DataType cqlType = getType(i);
    TypeCodec<Short> codec = codecRegistry().codecFor(cqlType, Short.class);
    if (codec instanceof PrimitiveShortCodec) {
      return ((PrimitiveShortCodec) codec).decodePrimitive(getBytesUnsafe(i), protocolVersion());
    } else {
      Short value = get(i, codec);
      return value == null ? 0 : value;
    }
  }

  /**
   * Returns the {@code i}th value as a Java instant.
   *
   * <p>By default, this works with CQL type {@code timestamp}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default Instant getInstant(int i) {
    return get(i, Instant.class);
  }

  /**
   * Returns the {@code i}th value as a Java local date.
   *
   * <p>By default, this works with CQL type {@code date}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default LocalDate getLocalDate(int i) {
    return get(i, LocalDate.class);
  }

  /**
   * Returns the {@code i}th value as a Java local time.
   *
   * <p>By default, this works with CQL type {@code time}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default LocalTime getLocalTime(int i) {
    return get(i, LocalTime.class);
  }

  /**
   * Returns the {@code i}th value as a Java byte buffer.
   *
   * <p>By default, this works with CQL type {@code blob}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default ByteBuffer getByteBuffer(int i) {
    return get(i, ByteBuffer.class);
  }

  /**
   * Returns the {@code i}th value as a Java string.
   *
   * <p>By default, this works with CQL types {@code text}, {@code varchar} and {@code ascii}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default String getString(int i) {
    return get(i, String.class);
  }

  /**
   * Returns the {@code i}th value as a Java big integer.
   *
   * <p>By default, this works with CQL type {@code varint}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default BigInteger getBigInteger(int i) {
    return get(i, BigInteger.class);
  }

  /**
   * Returns the {@code i}th value as a Java big decimal.
   *
   * <p>By default, this works with CQL type {@code decimal}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default BigDecimal getBigDecimal(int i) {
    return get(i, BigDecimal.class);
  }

  /**
   * Returns the {@code i}th value as a Java UUID.
   *
   * <p>By default, this works with CQL types {@code uuid} and {@code timeuuid}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default UUID getUuid(int i) {
    return get(i, UUID.class);
  }

  /**
   * Returns the {@code i}th value as a Java IP address.
   *
   * <p>By default, this works with CQL type {@code inet}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default InetAddress getInetAddress(int i) {
    return get(i, InetAddress.class);
  }

  /**
   * Returns the {@code i}th value as a duration.
   *
   * <p>By default, this works with CQL type {@code duration}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default CqlDuration getCqlDuration(int i) {
    return get(i, CqlDuration.class);
  }

  /**
   * Returns the {@code i}th value as a token.
   *
   * <p>Note that, for simplicity, this method relies on the CQL type of the column to pick the
   * correct token implementation. Therefore it must only be called on columns of the type that
   * matches the partitioner in use for this cluster: {@code bigint} for {@code Murmur3Partitioner},
   * {@code blob} for {@code ByteOrderedPartitioner}, and {@code varint} for {@code
   * RandomPartitioner}. Calling it for the wrong type will produce corrupt tokens that are unusable
   * with this driver instance.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws IllegalArgumentException if the column type can not be converted to a known token type.
   */
  @Nullable
  default Token getToken(int i) {
    DataType type = getType(i);
    // Simply enumerate all known implementations. This goes against the concept of TokenFactory,
    // but injecting the factory here is too much of a hassle.
    // The only issue is if someone uses a custom partitioner, but this is highly unlikely, and even
    // then they can get the value manually as a workaround.
    if (type.equals(DataTypes.BIGINT)) {
      return isNull(i) ? null : new Murmur3Token(getLong(i));
    } else if (type.equals(DataTypes.BLOB)) {
      return isNull(i) ? null : new ByteOrderedToken(getByteBuffer(i));
    } else if (type.equals(DataTypes.VARINT)) {
      return isNull(i) ? null : new RandomToken(getBigInteger(i));
    } else {
      throw new IllegalArgumentException("Can't convert CQL type " + type + " into a token");
    }
  }

  /**
   * Returns the {@code i}th value as a Java list.
   *
   * <p>By default, this works with CQL type {@code list}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex list types, use {@link #get(int, GenericType)}.
   *
   * <p>Apache Cassandra does not make any distinction between an empty collection and {@code null}.
   * Whether this method will return an empty collection or {@code null} will depend on the codec
   * used; by default, the driver's built-in codecs all return empty collections.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default <ElementT> List<ElementT> getList(int i, @NonNull Class<ElementT> elementsClass) {
    return get(i, GenericType.listOf(elementsClass));
  }

  /**
   * Returns the {@code i}th value as a Java set.
   *
   * <p>By default, this works with CQL type {@code set}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex set types, use {@link #get(int, GenericType)}.
   *
   * <p>Apache Cassandra does not make any distinction between an empty collection and {@code null}.
   * Whether this method will return an empty collection or {@code null} will depend on the codec
   * used; by default, the driver's built-in codecs all return empty collections.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default <ElementT> Set<ElementT> getSet(int i, @NonNull Class<ElementT> elementsClass) {
    return get(i, GenericType.setOf(elementsClass));
  }

  /**
   * Returns the {@code i}th value as a Java map.
   *
   * <p>By default, this works with CQL type {@code map}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex map types, use {@link #get(int, GenericType)}.
   *
   * <p>Apache Cassandra does not make any distinction between an empty collection and {@code null}.
   * Whether this method will return an empty collection or {@code null} will depend on the codec
   * used; by default, the driver's built-in codecs all return empty collections.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default <KeyT, ValueT> Map<KeyT, ValueT> getMap(
      int i, @NonNull Class<KeyT> keyClass, @NonNull Class<ValueT> valueClass) {
    return get(i, GenericType.mapOf(keyClass, valueClass));
  }

  /**
   * Returns the {@code i}th value as a user defined type value.
   *
   * <p>By default, this works with CQL user-defined types.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default UdtValue getUdtValue(int i) {
    return get(i, UdtValue.class);
  }

  /**
   * Returns the {@code i}th value as a tuple value.
   *
   * <p>By default, this works with CQL tuples.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  @Nullable
  default TupleValue getTupleValue(int i) {
    return get(i, TupleValue.class);
  }
}
