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

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
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

/** A data structure that provides methods to set its values via an integer index. */
public interface SettableByIndex<T extends SettableByIndex<T>> extends AccessibleByIndex {

  /**
   * Sets the raw binary representation of the {@code i}th value.
   *
   * <p>This is primarily for internal use; you'll likely want to use one of the typed setters
   * instead, to pass a higher-level Java representation.
   *
   * @param v the raw value, or {@code null} to set the CQL value {@code NULL}. For performance
   *     reasons, this is the actual instance used internally. If pass in a buffer that you're going
   *     to modify elsewhere in your application, make sure to {@link ByteBuffer#duplicate()
   *     duplicate} it beforehand. If you change the buffer's index or its contents in any way,
   *     further usage of this data will have unpredictable results.
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  T setBytesUnsafe(int i, ByteBuffer v);

  /**
   * Sets the {@code i}th value to CQL {@code NULL}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setToNull(int i) {
    return setBytesUnsafe(i, null);
  }

  /**
   * Sets the {@code i}th value, using the given codec for the conversion.
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
  default <V> T set(int i, V v, TypeCodec<V> codec) {
    return setBytesUnsafe(i, codec.encode(v, protocolVersion()));
  }

  /**
   * Sets the {@code i}th value, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>This variant is for generic Java types. If the target type is not generic, use {@link
   * #set(int, V, Class)} instead, which may perform slightly better.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <V> T set(int i, V v, GenericType<V> targetType) {
    DataType cqlType = getType(i);
    TypeCodec<V> codec = codecRegistry().codecFor(cqlType, targetType);
    return set(i, v, codec);
  }

  /**
   * Returns the {@code i}th value, converting it to the given Java type.
   *
   * <p>The {@link #codecRegistry()} will be used to look up a codec to handle the conversion.
   *
   * <p>If the target type is generic, use {@link #set(int, Object, GenericType)} instead.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   * @throws CodecNotFoundException if no codec can perform the conversion.
   */
  default <V> T set(int i, V v, Class<V> targetClass) {
    // This is duplicated from the GenericType variant, because we want to give the codec registry
    // a chance to process the unwrapped class directly, if it can do so in a more efficient way.
    DataType cqlType = getType(i);
    TypeCodec<V> codec = codecRegistry().codecFor(cqlType, targetClass);
    return set(i, v, codec);
  }

  /**
   * Sets the {@code i}th value to the provided Java primitive boolean.
   *
   * <p>By default, this works with CQL type {@code boolean}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Boolean.class)}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setBoolean(int i, boolean v) {
    DataType cqlType = getType(i);
    TypeCodec<Boolean> codec = codecRegistry().codecFor(cqlType, Boolean.class);
    return (codec instanceof PrimitiveBooleanCodec)
        ? setBytesUnsafe(i, ((PrimitiveBooleanCodec) codec).encodePrimitive(v, protocolVersion()))
        : set(i, v, codec);
  }

  /**
   * Sets the {@code i}th value to the provided Java primitive byte.
   *
   * <p>By default, this works with CQL type {@code tinyint}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Boolean.class)}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setByte(int i, byte v) {
    DataType cqlType = getType(i);
    TypeCodec<Byte> codec = codecRegistry().codecFor(cqlType, Byte.class);
    return (codec instanceof PrimitiveByteCodec)
        ? setBytesUnsafe(i, ((PrimitiveByteCodec) codec).encodePrimitive(v, protocolVersion()))
        : set(i, v, codec);
  }

  /**
   * Sets the {@code i}th value to the provided Java primitive double.
   *
   * <p>By default, this works with CQL type {@code double}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Double.class)}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setDouble(int i, double v) {
    DataType cqlType = getType(i);
    TypeCodec<Double> codec = codecRegistry().codecFor(cqlType, Double.class);
    return (codec instanceof PrimitiveDoubleCodec)
        ? setBytesUnsafe(i, ((PrimitiveDoubleCodec) codec).encodePrimitive(v, protocolVersion()))
        : set(i, v, codec);
  }

  /**
   * Sets the {@code i}th value to the provided Java primitive float.
   *
   * <p>By default, this works with CQL type {@code float}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Float.class)}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setFloat(int i, float v) {
    DataType cqlType = getType(i);
    TypeCodec<Float> codec = codecRegistry().codecFor(cqlType, Float.class);
    return (codec instanceof PrimitiveFloatCodec)
        ? setBytesUnsafe(i, ((PrimitiveFloatCodec) codec).encodePrimitive(v, protocolVersion()))
        : set(i, v, codec);
  }

  /**
   * Sets the {@code i}th value to the provided Java primitive integer.
   *
   * <p>By default, this works with CQL type {@code int}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Integer.class)}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setInt(int i, int v) {
    DataType cqlType = getType(i);
    TypeCodec<Integer> codec = codecRegistry().codecFor(cqlType, Integer.class);
    return (codec instanceof PrimitiveIntCodec)
        ? setBytesUnsafe(i, ((PrimitiveIntCodec) codec).encodePrimitive(v, protocolVersion()))
        : set(i, v, codec);
  }

  /**
   * Sets the {@code i}th value to the provided Java primitive long.
   *
   * <p>By default, this works with CQL types {@code bigint} and {@code counter}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Long.class)}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setLong(int i, long v) {
    DataType cqlType = getType(i);
    TypeCodec<Long> codec = codecRegistry().codecFor(cqlType, Long.class);
    return (codec instanceof PrimitiveLongCodec)
        ? setBytesUnsafe(i, ((PrimitiveLongCodec) codec).encodePrimitive(v, protocolVersion()))
        : set(i, v, codec);
  }

  /**
   * Sets the {@code i}th value to the provided Java primitive short.
   *
   * <p>By default, this works with CQL type {@code smallint}.
   *
   * <p>To set the value to CQL {@code NULL}, use {@link #setToNull(int)}, or {@code set(i, v,
   * Short.class)}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setShort(int i, short v) {
    DataType cqlType = getType(i);
    TypeCodec<Short> codec = codecRegistry().codecFor(cqlType, Short.class);
    return (codec instanceof PrimitiveShortCodec)
        ? setBytesUnsafe(i, ((PrimitiveShortCodec) codec).encodePrimitive(v, protocolVersion()))
        : set(i, v, codec);
  }

  /**
   * Sets the {@code i}th value to the provided Java instant.
   *
   * <p>By default, this works with CQL type {@code timestamp}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setInstant(int i, Instant v) {
    return set(i, v, Instant.class);
  }

  /**
   * Sets the {@code i}th value to the provided Java local date.
   *
   * <p>By default, this works with CQL type {@code date}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setLocalDate(int i, LocalDate v) {
    return set(i, v, LocalDate.class);
  }

  /**
   * Sets the {@code i}th value to the provided Java local time.
   *
   * <p>By default, this works with CQL type {@code time}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setLocalTime(int i, LocalTime v) {
    return set(i, v, LocalTime.class);
  }

  /**
   * Sets the {@code i}th value to the provided Java byte buffer.
   *
   * <p>By default, this works with CQL type {@code blob}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setByteBuffer(int i, ByteBuffer v) {
    return set(i, v, ByteBuffer.class);
  }

  /**
   * Sets the {@code i}th value to the provided Java string.
   *
   * <p>By default, this works with CQL types {@code text}, {@code varchar} and {@code ascii}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setString(int i, String v) {
    return set(i, v, String.class);
  }

  /**
   * Sets the {@code i}th value to the provided Java big integer.
   *
   * <p>By default, this works with CQL type {@code varint}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setBigInteger(int i, BigInteger v) {
    return set(i, v, BigInteger.class);
  }

  /**
   * Sets the {@code i}th value to the provided Java big decimal.
   *
   * <p>By default, this works with CQL type {@code decimal}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setBigDecimal(int i, BigDecimal v) {
    return set(i, v, BigDecimal.class);
  }

  /**
   * Sets the {@code i}th value to the provided Java UUID.
   *
   * <p>By default, this works with CQL types {@code uuid} and {@code timeuuid}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setUuid(int i, UUID v) {
    return set(i, v, UUID.class);
  }

  /**
   * Sets the {@code i}th value to the provided Java IP address.
   *
   * <p>By default, this works with CQL type {@code inet}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setInetAddress(int i, InetAddress v) {
    return set(i, v, InetAddress.class);
  }

  /**
   * Sets the {@code i}th value to the provided duration.
   *
   * <p>By default, this works with CQL type {@code duration}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setCqlDuration(int i, CqlDuration v) {
    return set(i, v, CqlDuration.class);
  }

  /**
   * Sets the {@code i}th value to the provided token.
   *
   * <p>This works with the CQL type matching the partitioner in use for this cluster: {@code
   * bigint} for {@code Murmur3Partitioner}, {@code blob} for {@code ByteOrderedPartitioner}, and
   * {@code varint} for {@code RandomPartitioner}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setToken(int i, Token v) {
    // Simply enumerate all known implementations. This goes against the concept of TokenFactory,
    // but injecting the factory here is too much of a hassle.
    // The only issue is if someone uses a custom partitioner, but this is highly unlikely, and even
    // then they can set the value manually as a workaround.
    if (v instanceof Murmur3Token) {
      return setLong(i, ((Murmur3Token) v).getValue());
    } else if (v instanceof ByteOrderedToken) {
      return setByteBuffer(i, ((ByteOrderedToken) v).getValue());
    } else if (v instanceof RandomToken) {
      return setBigInteger(i, ((RandomToken) v).getValue());
    } else {
      throw new IllegalArgumentException("Unsupported token type " + v.getClass());
    }
  }

  /**
   * Sets the {@code i}th value to the provided Java list.
   *
   * <p>By default, this works with CQL type {@code list}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex list types, use {@link #set(int, Object, GenericType)}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <V> T setList(int i, List<V> v, Class<V> elementsClass) {
    return set(i, v, GenericType.listOf(elementsClass));
  }

  /**
   * Sets the {@code i}th value to the provided Java set.
   *
   * <p>By default, this works with CQL type {@code set}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex set types, use {@link #set(int, Object, GenericType)}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <V> T setSet(int i, Set<V> v, Class<V> elementsClass) {
    return set(i, v, GenericType.setOf(elementsClass));
  }

  /**
   * Sets the {@code i}th value to the provided Java map.
   *
   * <p>By default, this works with CQL type {@code map}.
   *
   * <p>This method is provided for convenience when the element type is a non-generic type. For
   * more complex map types, use {@link #set(int, Object, GenericType)}.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default <K, V> T setMap(int i, Map<K, V> v, Class<K> keyClass, Class<V> valueClass) {
    return set(i, v, GenericType.mapOf(keyClass, valueClass));
  }

  /**
   * Sets the {@code i}th value to the provided user defined type value.
   *
   * <p>By default, this works with CQL user-defined types.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setUdtValue(int i, UdtValue v) {
    return set(i, v, UdtValue.class);
  }

  /**
   * Sets the {@code i}th value to the provided tuple value.
   *
   * <p>By default, this works with CQL tuples.
   *
   * @throws IndexOutOfBoundsException if the index is invalid.
   */
  default T setTupleValue(int i, TupleValue v) {
    return set(i, v, TupleValue.class);
  }
}
