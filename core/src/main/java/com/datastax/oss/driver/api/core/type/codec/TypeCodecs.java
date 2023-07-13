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
import com.datastax.oss.driver.internal.core.type.codec.ZonedTimestampCodec;
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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Constants and factory methods to obtain type codec instances. */
public class TypeCodecs {

  public static final PrimitiveBooleanCodec BOOLEAN = new BooleanCodec();
  public static final PrimitiveByteCodec TINYINT = new TinyIntCodec();
  public static final PrimitiveDoubleCodec DOUBLE = new DoubleCodec();
  public static final PrimitiveLongCodec COUNTER = new CounterCodec();
  public static final PrimitiveFloatCodec FLOAT = new FloatCodec();
  public static final PrimitiveIntCodec INT = new IntCodec();
  public static final PrimitiveLongCodec BIGINT = new BigIntCodec();
  public static final PrimitiveShortCodec SMALLINT = new SmallIntCodec();
  public static final TypeCodec<Instant> TIMESTAMP = new TimestampCodec();

  /**
   * A codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * ZonedDateTime}, using the system's {@linkplain ZoneId#systemDefault() default time zone} as its
   * source of time zone information.
   *
   * <p>Note that Apache Cassandra(R)'s timestamp type does not store any time zone; this codec is
   * provided merely as a convenience for users that need to deal with zoned timestamps in their
   * applications.
   *
   * @see #ZONED_TIMESTAMP_UTC
   * @see #zonedTimestampAt(ZoneId)
   */
  public static final TypeCodec<ZonedDateTime> ZONED_TIMESTAMP_SYSTEM = new ZonedTimestampCodec();

  /**
   * A codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * ZonedDateTime}, using {@link ZoneOffset#UTC} as its source of time zone information.
   *
   * <p>Note that Apache Cassandra(R)'s timestamp type does not store any time zone; this codec is
   * provided merely as a convenience for users that need to deal with zoned timestamps in their
   * applications.
   *
   * @see #ZONED_TIMESTAMP_SYSTEM
   * @see #zonedTimestampAt(ZoneId)
   */
  public static final TypeCodec<ZonedDateTime> ZONED_TIMESTAMP_UTC =
      new ZonedTimestampCodec(ZoneOffset.UTC);

  public static final TypeCodec<LocalDate> DATE = new DateCodec();
  public static final TypeCodec<LocalTime> TIME = new TimeCodec();
  public static final TypeCodec<ByteBuffer> BLOB = new BlobCodec();
  public static final TypeCodec<String> TEXT = new StringCodec(DataTypes.TEXT, Charsets.UTF_8);
  public static final TypeCodec<String> ASCII = new StringCodec(DataTypes.ASCII, Charsets.US_ASCII);
  public static final TypeCodec<BigInteger> VARINT = new VarIntCodec();
  public static final TypeCodec<BigDecimal> DECIMAL = new DecimalCodec();
  public static final TypeCodec<UUID> UUID = new UuidCodec();
  public static final TypeCodec<UUID> TIMEUUID = new TimeUuidCodec();
  public static final TypeCodec<InetAddress> INET = new InetCodec();
  public static final TypeCodec<CqlDuration> DURATION = new CqlDurationCodec();

  @NonNull
  public static TypeCodec<ByteBuffer> custom(@NonNull DataType cqlType) {
    Preconditions.checkArgument(cqlType instanceof CustomType, "cqlType must be a custom type");
    return new CustomCodec((CustomType) cqlType);
  }

  @NonNull
  public static <T> TypeCodec<List<T>> listOf(@NonNull TypeCodec<T> elementCodec) {
    return new ListCodec<>(DataTypes.listOf(elementCodec.getCqlType()), elementCodec);
  }

  @NonNull
  public static <T> TypeCodec<Set<T>> setOf(@NonNull TypeCodec<T> elementCodec) {
    return new SetCodec<>(DataTypes.setOf(elementCodec.getCqlType()), elementCodec);
  }

  @NonNull
  public static <K, V> TypeCodec<Map<K, V>> mapOf(
      @NonNull TypeCodec<K> keyCodec, @NonNull TypeCodec<V> valueCodec) {
    return new MapCodec<>(
        DataTypes.mapOf(keyCodec.getCqlType(), valueCodec.getCqlType()), keyCodec, valueCodec);
  }

  @NonNull
  public static TypeCodec<TupleValue> tupleOf(@NonNull TupleType cqlType) {
    return new TupleCodec(cqlType);
  }

  @NonNull
  public static TypeCodec<UdtValue> udtOf(@NonNull UserDefinedType cqlType) {
    return new UdtCodec(cqlType);
  }

  /**
   * Returns a codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
   * ZonedDateTime}, using the supplied {@link ZoneId} as its source of time zone information.
   *
   * <p>Note that Apache Cassandra(R)'s timestamp type does not store any time zone; the codecs
   * created by this method are provided merely as a convenience for users that need to deal with
   * zoned timestamps in their applications.
   *
   * @see #ZONED_TIMESTAMP_SYSTEM
   * @see #ZONED_TIMESTAMP_UTC
   */
  @NonNull
  public static TypeCodec<ZonedDateTime> zonedTimestampAt(@NonNull ZoneId timeZone) {
    return new ZonedTimestampCodec(timeZone);
  }
}
