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
package com.datastax.oss.driver.api.core.type.codec;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.codec.BigIntCodec;
import com.datastax.oss.driver.internal.core.type.codec.BlobCodec;
import com.datastax.oss.driver.internal.core.type.codec.BooleanCodec;
import com.datastax.oss.driver.internal.core.type.codec.CounterCodec;
import com.datastax.oss.driver.internal.core.type.codec.CqlDurationCodec;
import com.datastax.oss.driver.internal.core.type.codec.CustomCodec;
import com.datastax.oss.driver.internal.core.type.codec.DateCodec;
import com.datastax.oss.driver.internal.core.type.codec.DecimalCodec;
import com.datastax.oss.driver.internal.core.type.codec.DoubleCodec;
import com.datastax.oss.driver.internal.core.type.codec.FloatCodec;
import com.datastax.oss.driver.internal.core.type.codec.InetCodec;
import com.datastax.oss.driver.internal.core.type.codec.IntCodec;
import com.datastax.oss.driver.internal.core.type.codec.ListCodec;
import com.datastax.oss.driver.internal.core.type.codec.MapCodec;
import com.datastax.oss.driver.internal.core.type.codec.SetCodec;
import com.datastax.oss.driver.internal.core.type.codec.SmallIntCodec;
import com.datastax.oss.driver.internal.core.type.codec.StringCodec;
import com.datastax.oss.driver.internal.core.type.codec.TimeCodec;
import com.datastax.oss.driver.internal.core.type.codec.TimeUuidCodec;
import com.datastax.oss.driver.internal.core.type.codec.TimestampCodec;
import com.datastax.oss.driver.internal.core.type.codec.TinyIntCodec;
import com.datastax.oss.driver.internal.core.type.codec.TupleCodec;
import com.datastax.oss.driver.internal.core.type.codec.UdtCodec;
import com.datastax.oss.driver.internal.core.type.codec.UuidCodec;
import com.datastax.oss.driver.internal.core.type.codec.VarIntCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Constants and factory methods to obtain instances of the driver's default type codecs.
 *
 * <p>See also {@link ExtraTypeCodecs} for additional codecs that you can register with your session
 * to handle different type mappings.
 */
public class TypeCodecs {

  /** The default codec that maps CQL type {@code boolean} to Java's {@code boolean}. */
  public static final PrimitiveBooleanCodec BOOLEAN = new BooleanCodec();

  /** The default codec that maps CQL type {@code tinyint} to Java's {@code byte}. */
  public static final PrimitiveByteCodec TINYINT = new TinyIntCodec();

  /** The default codec that maps CQL type {@code double} to Java's {@code double}. */
  public static final PrimitiveDoubleCodec DOUBLE = new DoubleCodec();

  /** The default codec that maps CQL type {@code counter} to Java's {@code long}. */
  public static final PrimitiveLongCodec COUNTER = new CounterCodec();

  /** The default codec that maps CQL type {@code float} to Java's {@code float}. */
  public static final PrimitiveFloatCodec FLOAT = new FloatCodec();

  /** The default codec that maps CQL type {@code int} to Java's {@code int}. */
  public static final PrimitiveIntCodec INT = new IntCodec();

  /** The default codec that maps CQL type {@code bigint} to Java's {@code long}. */
  public static final PrimitiveLongCodec BIGINT = new BigIntCodec();

  /** The default codec that maps CQL type {@code smallint} to Java's {@code short}. */
  public static final PrimitiveShortCodec SMALLINT = new SmallIntCodec();

  /**
   * The default codec that maps CQL type {@code timestamp} to Java's {@link Instant}, using the
   * system's default time zone to parse and format CQL literals.
   *
   * <p>This codec uses the system's {@linkplain ZoneId#systemDefault() default time zone} as its
   * source of time zone information when formatting values as CQL literals, or parsing CQL literals
   * that do not have any time zone indication. Note that this only applies to the {@link
   * TypeCodec#format(Object)} and {@link TypeCodec#parse(String)} methods; regular encoding and
   * decoding, like setting a value on a bound statement or reading a column from a row, are not
   * affected by the time zone.
   *
   * <p>If you need a different time zone, consider other codecs in {@link ExtraTypeCodecs}, or call
   * {@link ExtraTypeCodecs#timestampAt(ZoneId)} instead.
   *
   * @see ExtraTypeCodecs#TIMESTAMP_UTC
   * @see ExtraTypeCodecs#timestampAt(ZoneId)
   */
  public static final TypeCodec<Instant> TIMESTAMP = new TimestampCodec();

  /** The default codec that maps CQL type {@code date} to Java's {@link LocalDate}. */
  public static final TypeCodec<LocalDate> DATE = new DateCodec();

  /** The default codec that maps CQL type {@code time} to Java's {@link LocalTime}. */
  public static final TypeCodec<LocalTime> TIME = new TimeCodec();

  /**
   * The default codec that maps CQL type {@code blob} to Java's {@link ByteBuffer}.
   *
   * <p>If you are looking for a codec mapping CQL type {@code blob} to the Java type {@code
   * byte[]}, you should use {@link ExtraTypeCodecs#BLOB_TO_ARRAY} instead.
   *
   * <p>If you are looking for a codec mapping CQL type {@code list<tinyint>} to the Java type
   * {@code byte[]}, you should use {@link ExtraTypeCodecs#BYTE_LIST_TO_ARRAY} instead.
   *
   * @see ExtraTypeCodecs#BLOB_TO_ARRAY
   * @see ExtraTypeCodecs#BYTE_LIST_TO_ARRAY
   */
  public static final TypeCodec<ByteBuffer> BLOB = new BlobCodec();

  /** The default codec that maps CQL type {@code text} to Java's {@link String}. */
  public static final TypeCodec<String> TEXT = new StringCodec(DataTypes.TEXT, Charsets.UTF_8);
  /** The default codec that maps CQL type {@code ascii} to Java's {@link String}. */
  public static final TypeCodec<String> ASCII = new StringCodec(DataTypes.ASCII, Charsets.US_ASCII);
  /** The default codec that maps CQL type {@code varint} to Java's {@link BigInteger}. */
  public static final TypeCodec<BigInteger> VARINT = new VarIntCodec();
  /** The default codec that maps CQL type {@code decimal} to Java's {@link BigDecimal}. */
  public static final TypeCodec<BigDecimal> DECIMAL = new DecimalCodec();
  /** The default codec that maps CQL type {@code uuid} to Java's {@link UUID}. */
  public static final TypeCodec<UUID> UUID = new UuidCodec();
  /** The default codec that maps CQL type {@code timeuuid} to Java's {@link UUID}. */
  public static final TypeCodec<UUID> TIMEUUID = new TimeUuidCodec();
  /** The default codec that maps CQL type {@code inet} to Java's {@link InetAddress}. */
  public static final TypeCodec<InetAddress> INET = new InetCodec();
  /** The default codec that maps CQL type {@code duration} to the driver's {@link CqlDuration}. */
  public static final TypeCodec<CqlDuration> DURATION = new CqlDurationCodec();

  /**
   * Builds a new codec that maps a CQL custom type to Java's {@link ByteBuffer}.
   *
   * @param cqlType the fully-qualified name of the custom type.
   */
  @NonNull
  public static TypeCodec<ByteBuffer> custom(@NonNull DataType cqlType) {
    Preconditions.checkArgument(cqlType instanceof CustomType, "cqlType must be a custom type");
    return new CustomCodec((CustomType) cqlType);
  }

  /**
   * Builds a new codec that maps a CQL list to a Java list, using the given codec to map each
   * element.
   */
  @NonNull
  public static <T> TypeCodec<List<T>> listOf(@NonNull TypeCodec<T> elementCodec) {
    return new ListCodec<>(DataTypes.listOf(elementCodec.getCqlType()), elementCodec);
  }

  /**
   * Builds a new codec that maps a CQL set to a Java set, using the given codec to map each
   * element.
   */
  @NonNull
  public static <T> TypeCodec<Set<T>> setOf(@NonNull TypeCodec<T> elementCodec) {
    return new SetCodec<>(DataTypes.setOf(elementCodec.getCqlType()), elementCodec);
  }

  /**
   * Builds a new codec that maps a CQL map to a Java map, using the given codecs to map each key
   * and value.
   */
  @NonNull
  public static <K, V> TypeCodec<Map<K, V>> mapOf(
      @NonNull TypeCodec<K> keyCodec, @NonNull TypeCodec<V> valueCodec) {
    return new MapCodec<>(
        DataTypes.mapOf(keyCodec.getCqlType(), valueCodec.getCqlType()), keyCodec, valueCodec);
  }

  /**
   * Builds a new codec that maps a CQL tuple to the driver's {@link TupleValue}, for the given type
   * definition.
   *
   * <p>Note that the components of a {@link TupleValue} are stored in their encoded form. They are
   * encoded/decoded on the fly when you set or get them, using the codec registry.
   */
  @NonNull
  public static TypeCodec<TupleValue> tupleOf(@NonNull TupleType cqlType) {
    return new TupleCodec(cqlType);
  }

  /**
   * Builds a new codec that maps a CQL user defined type to the driver's {@link UdtValue}, for the
   * given type definition.
   *
   * <p>Note that the fields of a {@link UdtValue} are stored in their encoded form. They are
   * encoded/decoded on the fly when you set or get them, using the codec registry.
   */
  @NonNull
  public static TypeCodec<UdtValue> udtOf(@NonNull UserDefinedType cqlType) {
    return new UdtCodec(cqlType);
  }

  /**
   * An alias for {@link ExtraTypeCodecs#ZONED_TIMESTAMP_SYSTEM}.
   *
   * <p>This exists for historical reasons: the constant was originally defined in this class, but
   * technically it belongs to {@link ExtraTypeCodecs} because this is not a built-in mapping.
   */
  public static final TypeCodec<ZonedDateTime> ZONED_TIMESTAMP_SYSTEM =
      ExtraTypeCodecs.ZONED_TIMESTAMP_SYSTEM;

  /**
   * An alias for {@link ExtraTypeCodecs#ZONED_TIMESTAMP_UTC}.
   *
   * <p>This exists for historical reasons: the constant was originally defined in this class, but
   * technically it belongs to {@link ExtraTypeCodecs} because this is not a built-in mapping.
   */
  public static final TypeCodec<ZonedDateTime> ZONED_TIMESTAMP_UTC =
      ExtraTypeCodecs.ZONED_TIMESTAMP_UTC;

  /**
   * An alias for {@link ExtraTypeCodecs#zonedTimestampAt(ZoneId)}.
   *
   * <p>This exists for historical reasons: the method was originally defined in this class, but
   * technically it belongs to {@link ExtraTypeCodecs} because this is not a built-in mapping.
   */
  @NonNull
  public static TypeCodec<ZonedDateTime> zonedTimestampAt(@NonNull ZoneId timeZone) {
    return ExtraTypeCodecs.zonedTimestampAt(timeZone);
  }
}
