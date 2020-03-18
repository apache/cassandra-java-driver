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

import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.SimpleBlobCodec;
import com.datastax.oss.driver.internal.core.type.codec.TimestampCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.OptionalCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.BooleanListToArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.ByteListToArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.DoubleListToArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.FloatListToArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.IntListToArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.LongListToArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.ObjectListToArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.array.ShortListToArrayCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.enums.EnumNameCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.enums.EnumOrdinalCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.json.JsonCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.time.LocalTimestampCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.time.PersistentZonedTimestampCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.time.TimestampMillisCodec;
import com.datastax.oss.driver.internal.core.type.codec.extras.time.ZonedTimestampCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

/**
 * Additional codecs that can be registered to handle different type mappings.
 *
 * @see SessionBuilder#addTypeCodecs(TypeCodec[])
 * @see MutableCodecRegistry#register(TypeCodec)
 */
public class ExtraTypeCodecs {

  /**
   * A codec that maps CQL type {@code timestamp} to Java's {@link Instant}, using the UTC time zone
   * to parse and format CQL literals.
   *
   * <p>This codec uses {@link ZoneOffset#UTC} as its source of time zone information when
   * formatting values as CQL literals, or parsing CQL literals that do not have any time zone
   * indication. Note that this only applies to the {@link TypeCodec#format(Object)} and {@link
   * TypeCodec#parse(String)} methods; regular encoding and decoding, like setting a value on a
   * bound statement or reading a column from a row, are not affected by the time zone.
   *
   * <p>If you need a different time zone, consider other constants in this class, or call {@link
   * ExtraTypeCodecs#timestampAt(ZoneId)} instead.
   *
   * @see TypeCodecs#TIMESTAMP
   * @see ExtraTypeCodecs#timestampAt(ZoneId)
   */
  public static final TypeCodec<Instant> TIMESTAMP_UTC = new TimestampCodec(ZoneOffset.UTC);

  /**
   * A codec that maps CQL type {@code timestamp} to Java's {@code long}, representing the number of
   * milliseconds since the Epoch, using the system's default time zone to parse and format CQL
   * literals.
   *
   * <p>This codec uses the system's {@linkplain ZoneId#systemDefault() default time zone} as its
   * source of time zone information when formatting values as CQL literals, or parsing CQL literals
   * that do not have any time zone indication. Note that this only applies to the {@link
   * TypeCodec#format(Object)} and {@link TypeCodec#parse(String)} methods; regular encoding and
   * decoding, like setting a value on a bound statement or reading a column from a row, are not
   * affected by the time zone.
   *
   * <p>If you need a different time zone, consider other constants in this class, or call {@link
   * #timestampMillisAt(ZoneId)} instead.
   *
   * <p>This codec can serve as a replacement for the driver's built-in {@linkplain
   * TypeCodecs#TIMESTAMP timestamp} codec, when application code prefers to deal with raw
   * milliseconds than with {@link Instant} instances.
   *
   * @see #TIMESTAMP_MILLIS_UTC
   * @see #timestampMillisAt(ZoneId)
   */
  public static final PrimitiveLongCodec TIMESTAMP_MILLIS_SYSTEM = new TimestampMillisCodec();

  /**
   * A codec that maps CQL type {@code timestamp} to Java's {@code long}, representing the number of
   * milliseconds since the Epoch, using the UTC time zone to parse and format CQL literals.
   *
   * <p>This codec uses {@link ZoneOffset#UTC} as its source of time zone information when
   * formatting values as CQL literals, or parsing CQL literals that do not have any time zone
   * indication. Note that this only applies to the {@link TypeCodec#format(Object)} and {@link
   * TypeCodec#parse(String)} methods; regular encoding and decoding, like setting a value on a
   * bound statement or reading a column from a row, are not affected by the time zone.
   *
   * <p>If you need a different time zone, consider other constants in this class, or call {@link
   * #timestampMillisAt(ZoneId)} instead.
   *
   * <p>This codec can serve as a replacement for the driver's built-in {@linkplain
   * TypeCodecs#TIMESTAMP timestamp} codec, when application code prefers to deal with raw
   * milliseconds than with {@link Instant} instances.
   *
   * @see #TIMESTAMP_MILLIS_SYSTEM
   * @see #timestampMillisAt(ZoneId)
   */
  public static final PrimitiveLongCodec TIMESTAMP_MILLIS_UTC =
      new TimestampMillisCodec(ZoneOffset.UTC);

  /**
   * A codec that maps CQL type {@code timestamp} to Java's {@link ZonedDateTime}, using the
   * system's default time zone.
   *
   * <p>This codec uses the system's {@linkplain ZoneId#systemDefault() default time zone} as its
   * source of time zone information when encoding or decoding. If you need a different time zone,
   * consider using other constants in this class, or call {@link #zonedTimestampAt(ZoneId)}
   * instead.
   *
   * <p>Note that CQL type {@code timestamp} type does not store any time zone; this codec is
   * provided merely as a convenience for users that need to deal with zoned timestamps in their
   * applications.
   *
   * @see #ZONED_TIMESTAMP_UTC
   * @see #ZONED_TIMESTAMP_PERSISTED
   * @see #zonedTimestampAt(ZoneId)
   */
  public static final TypeCodec<ZonedDateTime> ZONED_TIMESTAMP_SYSTEM = new ZonedTimestampCodec();

  /**
   * A codec that maps CQL type {@code timestamp} to Java's {@link ZonedDateTime}, using the UTC
   * time zone.
   *
   * <p>This codec uses {@link ZoneOffset#UTC} as its source of time zone information when encoding
   * or decoding. If you need a different time zone, consider using other constants in this class,
   * or call {@link #zonedTimestampAt(ZoneId)} instead.
   *
   * <p>Note that CQL type {@code timestamp} type does not store any time zone; this codec is
   * provided merely as a convenience for users that need to deal with zoned timestamps in their
   * applications.
   *
   * @see #ZONED_TIMESTAMP_SYSTEM
   * @see #ZONED_TIMESTAMP_PERSISTED
   * @see #zonedTimestampAt(ZoneId)
   */
  public static final TypeCodec<ZonedDateTime> ZONED_TIMESTAMP_UTC =
      new ZonedTimestampCodec(ZoneOffset.UTC);

  /**
   * A codec that maps CQL type {@code tuple<timestamp,text>} to Java's {@link ZonedDateTime},
   * providing a pattern for maintaining timezone information in Cassandra.
   *
   * <p>Since CQL type {@code timestamp} does not store any time zone, it is persisted separately in
   * the {@code text} field of the tuple, and so when the value is read back the original timezone
   * it was written with is preserved.
   *
   * @see #ZONED_TIMESTAMP_SYSTEM
   * @see #ZONED_TIMESTAMP_UTC
   * @see #zonedTimestampAt(ZoneId)
   */
  public static final TypeCodec<ZonedDateTime> ZONED_TIMESTAMP_PERSISTED =
      new PersistentZonedTimestampCodec();

  /**
   * A codec that maps CQL type {@code timestamp} to Java's {@link LocalDateTime}, using the
   * system's default time zone.
   *
   * <p>This codec uses the system's {@linkplain ZoneId#systemDefault() default time zone} as its
   * source of time zone information when encoding or decoding. If you need a different time zone,
   * consider using other constants in this class, or call {@link #localTimestampAt(ZoneId)}
   * instead.
   *
   * <p>Note that CQL type {@code timestamp} does not store any time zone; this codec is provided
   * merely as a convenience for users that need to deal with local date-times in their
   * applications.
   *
   * @see #LOCAL_TIMESTAMP_UTC
   * @see #localTimestampAt(ZoneId)
   */
  public static final TypeCodec<LocalDateTime> LOCAL_TIMESTAMP_SYSTEM = new LocalTimestampCodec();

  /**
   * A codec that maps CQL type {@code timestamp} to Java's {@link LocalDateTime}, using the UTC
   * time zone.
   *
   * <p>This codec uses {@link ZoneOffset#UTC} as its source of time zone information when encoding
   * or decoding. If you need a different time zone, consider using other constants in this class,
   * or call {@link #localTimestampAt(ZoneId)} instead.
   *
   * <p>Note that CQL type {@code timestamp} does not store any time zone; this codec is provided
   * merely as a convenience for users that need to deal with local date-times in their
   * applications.
   *
   * @see #LOCAL_TIMESTAMP_SYSTEM
   * @see #localTimestampAt(ZoneId)
   */
  public static final TypeCodec<LocalDateTime> LOCAL_TIMESTAMP_UTC =
      new LocalTimestampCodec(ZoneOffset.UTC);

  /**
   * A codec that maps CQL type {@code blob} to Java's {@code byte[]}.
   *
   * <p>If you are looking for a codec mapping CQL type {@code blob} to the Java type {@link
   * ByteBuffer}, you should use {@link TypeCodecs#BLOB} instead.
   *
   * <p>If you are looking for a codec mapping CQL type {@code list<tinyint} to the Java type {@code
   * byte[]}, you should use {@link #BYTE_LIST_TO_ARRAY} instead.
   *
   * @see TypeCodecs#BLOB
   * @see #BYTE_LIST_TO_ARRAY
   */
  public static final TypeCodec<byte[]> BLOB_TO_ARRAY = new SimpleBlobCodec();

  /**
   * A codec that maps CQL type {@code list<boolean>} to Java's {@code boolean[]}.
   *
   * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
   * {@code boolean[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code
   * boolean} values; it also instantiates arrays without the need for an intermediary Java {@code
   * List} object.
   */
  public static final TypeCodec<boolean[]> BOOLEAN_LIST_TO_ARRAY = new BooleanListToArrayCodec();

  /**
   * A codec that maps CQL type {@code list<tinyint>} to Java's {@code byte[]}.
   *
   * <p>This codec is not suitable for reading CQL blobs as byte arrays. If you are looking for a
   * codec for the CQL type {@code blob}, you should use {@link TypeCodecs#BLOB} or {@link
   * ExtraTypeCodecs#BLOB_TO_ARRAY} instead.
   *
   * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
   * {@code byte[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code
   * byte} values; it also instantiates arrays without the need for an intermediary Java {@code
   * List} object.
   *
   * @see TypeCodecs#BLOB
   * @see ExtraTypeCodecs#BLOB_TO_ARRAY
   */
  public static final TypeCodec<byte[]> BYTE_LIST_TO_ARRAY = new ByteListToArrayCodec();

  /**
   * A codec that maps CQL type {@code list<smallint>} to Java's {@code short[]}.
   *
   * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
   * {@code short[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code
   * short} values; it also instantiates arrays without the need for an intermediary Java {@code
   * List} object.
   */
  public static final TypeCodec<short[]> SHORT_LIST_TO_ARRAY = new ShortListToArrayCodec();

  /**
   * A codec that maps CQL type {@code list<int>} to Java's {@code int[]}.
   *
   * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
   * {@code int[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code int}
   * values; it also instantiates arrays without the need for an intermediary Java {@code List}
   * object.
   */
  public static final TypeCodec<int[]> INT_LIST_TO_ARRAY = new IntListToArrayCodec();

  /**
   * A codec that maps CQL type {@code list<bigint>} to Java's {@code long[]}.
   *
   * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
   * {@code long[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code
   * long} values; it also instantiates arrays without the need for an intermediary Java {@code
   * List} object.
   */
  public static final TypeCodec<long[]> LONG_LIST_TO_ARRAY = new LongListToArrayCodec();

  /**
   * A codec that maps CQL type {@code list<float>} to Java's {@code float[]}.
   *
   * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
   * {@code float[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code
   * float} values; it also instantiates arrays without the need for an intermediary Java {@code
   * List} object.
   */
  public static final TypeCodec<float[]> FLOAT_LIST_TO_ARRAY = new FloatListToArrayCodec();

  /**
   * A codec that maps CQL type {@code list<double>} to Java's {@code double[]}.
   *
   * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
   * {@code double[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code
   * double} values; it also instantiates arrays without the need for an intermediary Java {@code
   * List} object.
   */
  public static final TypeCodec<double[]> DOUBLE_LIST_TO_ARRAY = new DoubleListToArrayCodec();

  /**
   * Builds a new codec that maps CQL type {@code timestamp} to Java's {@link Instant}, using the
   * given time zone to parse and format CQL literals.
   *
   * <p>This codec uses the supplied {@link ZoneId} as its source of time zone information when
   * formatting values as CQL literals, or parsing CQL literals that do not have any time zone
   * indication. Note that this only applies to the {@link TypeCodec#format(Object)} and {@link
   * TypeCodec#parse(String)} methods; regular encoding and decoding, like setting a value on a
   * bound statement or reading a column from a row, are not affected by the time zone.
   *
   * @see TypeCodecs#TIMESTAMP
   * @see ExtraTypeCodecs#TIMESTAMP_UTC
   */
  @NonNull
  public static TypeCodec<Instant> timestampAt(@NonNull ZoneId timeZone) {
    return new TimestampCodec(timeZone);
  }

  /**
   * Builds a new codec that maps CQL type {@code timestamp} to Java's {@code long}, representing
   * the number of milliseconds since the Epoch, using the given time zone to parse and format CQL
   * literals.
   *
   * <p>This codec uses the supplied {@link ZoneId} as its source of time zone information when
   * formatting values as CQL literals, or parsing CQL literals that do not have any time zone
   * indication. Note that this only applies to the {@link TypeCodec#format(Object)} and {@link
   * TypeCodec#parse(String)} methods; regular encoding and decoding, like setting a value on a
   * bound statement or reading a column from a row, are not affected by the time zone.
   *
   * <p>This codec can serve as a replacement for the driver's built-in {@linkplain
   * TypeCodecs#TIMESTAMP timestamp} codec, when application code prefers to deal with raw
   * milliseconds than with {@link Instant} instances.
   *
   * @see ExtraTypeCodecs#TIMESTAMP_MILLIS_SYSTEM
   * @see ExtraTypeCodecs#TIMESTAMP_MILLIS_UTC
   */
  @NonNull
  public static PrimitiveLongCodec timestampMillisAt(@NonNull ZoneId timeZone) {
    return new TimestampMillisCodec(timeZone);
  }

  /**
   * Builds a new codec that maps CQL type {@code timestamp} to Java's {@link ZonedDateTime}.
   *
   * <p>This codec uses the supplied {@link ZoneId} as its source of time zone information when
   * encoding or decoding.
   *
   * <p>Note that CQL type {@code timestamp} does not store any time zone; the codecs created by
   * this method are provided merely as a convenience for users that need to deal with zoned
   * timestamps in their applications.
   *
   * @see ExtraTypeCodecs#ZONED_TIMESTAMP_SYSTEM
   * @see ExtraTypeCodecs#ZONED_TIMESTAMP_UTC
   * @see ExtraTypeCodecs#ZONED_TIMESTAMP_PERSISTED
   */
  @NonNull
  public static TypeCodec<ZonedDateTime> zonedTimestampAt(@NonNull ZoneId timeZone) {
    return new ZonedTimestampCodec(timeZone);
  }

  /**
   * Builds a new codec that maps CQL type {@code timestamp} to Java's {@link LocalDateTime}.
   *
   * <p>This codec uses the supplied {@link ZoneId} as its source of time zone information when
   * encoding or decoding.
   *
   * <p>Note that CQL type {@code timestamp} does not store any time zone; the codecs created by
   * this method are provided merely as a convenience for users that need to deal with local
   * date-times in their applications.
   *
   * @see ExtraTypeCodecs#LOCAL_TIMESTAMP_UTC
   * @see #localTimestampAt(ZoneId)
   */
  @NonNull
  public static TypeCodec<LocalDateTime> localTimestampAt(@NonNull ZoneId timeZone) {
    return new LocalTimestampCodec(timeZone);
  }

  /**
   * Builds a new codec that maps a CQL list to a Java array. Encoding and decoding of elements in
   * the array is delegated to the provided element codec.
   *
   * <p>This method is not suitable for Java primitive arrays. Use {@link
   * ExtraTypeCodecs#BOOLEAN_LIST_TO_ARRAY}, {@link ExtraTypeCodecs#BYTE_LIST_TO_ARRAY}, {@link
   * ExtraTypeCodecs#SHORT_LIST_TO_ARRAY}, {@link ExtraTypeCodecs#INT_LIST_TO_ARRAY}, {@link
   * ExtraTypeCodecs#LONG_LIST_TO_ARRAY}, {@link ExtraTypeCodecs#FLOAT_LIST_TO_ARRAY} or {@link
   * ExtraTypeCodecs#DOUBLE_LIST_TO_ARRAY} instead.
   */
  @NonNull
  public static <T> TypeCodec<T[]> listToArrayOf(@NonNull TypeCodec<T> elementCodec) {
    return new ObjectListToArrayCodec<>(elementCodec);
  }

  /**
   * Builds a new codec that maps CQL type {@code int} to a Java Enum, according to its constants'
   * {@linkplain Enum#ordinal() ordinals} (<b>STRONGLY discouraged, see explanations below)</b>.
   *
   * <p>This method is provided for compatibility with driver 3, but we strongly recommend against
   * it. Relying on enum ordinals is a bad practice: any reordering of the enum constants, or
   * insertion of a new constant before the end, will change the ordinals. The codec will keep
   * working, but start inserting different codes and corrupting your data.
   *
   * <p>{@link #enumNamesOf(Class)} is a safer alternative, as it is not dependent on the constant
   * order. If you still want to use integer codes for storage efficiency, we recommend implementing
   * an explicit mapping (for example with a {@code toCode()} method on your enum type). It is then
   * fairly straightforward to implement a codec with {@link MappingCodec}, using {@link
   * TypeCodecs#INT} as the "inner" codec.
   */
  @NonNull
  public static <EnumT extends Enum<EnumT>> TypeCodec<EnumT> enumOrdinalsOf(
      @NonNull Class<EnumT> enumClass) {
    return new EnumOrdinalCodec<>(enumClass);
  }

  /**
   * Builds a new codec that maps CQL type {@code text} to a Java Enum, according to its constants'
   * programmatic {@linkplain Enum#name() names}.
   *
   * @see #enumOrdinalsOf(Class)
   */
  @NonNull
  public static <EnumT extends Enum<EnumT>> TypeCodec<EnumT> enumNamesOf(
      @NonNull Class<EnumT> enumClass) {
    return new EnumNameCodec<>(enumClass);
  }

  /**
   * Builds a new codec that wraps another codec's Java type into {@link Optional} instances
   * (mapping CQL null to {@link Optional#empty()}).
   */
  @NonNull
  public static <T> TypeCodec<Optional<T>> optionalOf(@NonNull TypeCodec<T> innerCodec) {
    return new OptionalCodec<>(innerCodec);
  }

  /**
   * Builds a new codec that maps CQL type {@code text} to the given Java type, using JSON
   * serialization with a default Jackson mapper.
   *
   * @see <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Library</a>
   */
  @NonNull
  public static <T> TypeCodec<T> json(@NonNull GenericType<T> javaType) {
    return new JsonCodec<>(javaType);
  }

  /**
   * Builds a new codec that maps CQL type {@code text} to the given Java type, using JSON
   * serialization with a default Jackson mapper.
   *
   * @see <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Library</a>
   */
  @NonNull
  public static <T> TypeCodec<T> json(@NonNull Class<T> javaType) {
    return new JsonCodec<>(javaType);
  }

  /**
   * Builds a new codec that maps CQL type {@code text} to the given Java type, using JSON
   * serialization with the provided Jackson mapper.
   *
   * @see <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Library</a>
   */
  @NonNull
  public static <T> TypeCodec<T> json(
      @NonNull GenericType<T> javaType, @NonNull ObjectMapper objectMapper) {
    return new JsonCodec<>(javaType, objectMapper);
  }

  /**
   * Builds a new codec that maps CQL type {@code text} to the given Java type, using JSON
   * serialization with the provided Jackson mapper.
   *
   * @see <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Library</a>
   */
  @NonNull
  public static <T> TypeCodec<T> json(
      @NonNull Class<T> javaType, @NonNull ObjectMapper objectMapper) {
    return new JsonCodec<>(javaType, objectMapper);
  }
}
