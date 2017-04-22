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
package com.datastax.oss.driver.api.type.codec;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.type.CustomType;
import com.datastax.oss.driver.api.type.DataType;
import com.datastax.oss.driver.api.type.DataTypes;
import com.datastax.oss.driver.api.type.TupleType;
import com.datastax.oss.driver.api.type.UserDefinedType;
import com.datastax.oss.driver.internal.type.codec.BigIntCodec;
import com.datastax.oss.driver.internal.type.codec.BlobCodec;
import com.datastax.oss.driver.internal.type.codec.BooleanCodec;
import com.datastax.oss.driver.internal.type.codec.CounterCodec;
import com.datastax.oss.driver.internal.type.codec.CqlDurationCodec;
import com.datastax.oss.driver.internal.type.codec.CustomCodec;
import com.datastax.oss.driver.internal.type.codec.DateCodec;
import com.datastax.oss.driver.internal.type.codec.DecimalCodec;
import com.datastax.oss.driver.internal.type.codec.DoubleCodec;
import com.datastax.oss.driver.internal.type.codec.FloatCodec;
import com.datastax.oss.driver.internal.type.codec.InetCodec;
import com.datastax.oss.driver.internal.type.codec.IntCodec;
import com.datastax.oss.driver.internal.type.codec.ListCodec;
import com.datastax.oss.driver.internal.type.codec.MapCodec;
import com.datastax.oss.driver.internal.type.codec.SetCodec;
import com.datastax.oss.driver.internal.type.codec.SmallIntCodec;
import com.datastax.oss.driver.internal.type.codec.StringCodec;
import com.datastax.oss.driver.internal.type.codec.TimeCodec;
import com.datastax.oss.driver.internal.type.codec.TimeUuidCodec;
import com.datastax.oss.driver.internal.type.codec.TimestampCodec;
import com.datastax.oss.driver.internal.type.codec.TinyIntCodec;
import com.datastax.oss.driver.internal.type.codec.TupleCodec;
import com.datastax.oss.driver.internal.type.codec.UdtCodec;
import com.datastax.oss.driver.internal.type.codec.UuidCodec;
import com.datastax.oss.driver.internal.type.codec.VarIntCodec;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
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

  public static TypeCodec<ByteBuffer> custom(DataType cqlType) {
    Preconditions.checkArgument(cqlType instanceof CustomType, "cqlType must be a custom type");
    return new CustomCodec((CustomType) cqlType);
  }

  public static <T> TypeCodec<List<T>> listOf(TypeCodec<T> elementCodec) {
    return new ListCodec<>(DataTypes.listOf(elementCodec.getCqlType()), elementCodec);
  }

  public static <T> TypeCodec<Set<T>> setOf(TypeCodec<T> elementCodec) {
    return new SetCodec<>(DataTypes.setOf(elementCodec.getCqlType()), elementCodec);
  }

  public static <K, V> TypeCodec<Map<K, V>> mapOf(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec) {
    return new MapCodec<>(
        DataTypes.mapOf(keyCodec.getCqlType(), valueCodec.getCqlType()), keyCodec, valueCodec);
  }

  public static TypeCodec<TupleValue> tupleOf(TupleType cqlType) {
    return new TupleCodec(cqlType);
  }

  public static TypeCodec<UdtValue> udtOf(UserDefinedType cqlType) {
    return new UdtCodec(cqlType);
  }
}
