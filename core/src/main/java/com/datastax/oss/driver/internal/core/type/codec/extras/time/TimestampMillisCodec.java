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
package com.datastax.oss.driver.internal.core.type.codec.extras.time;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveLongCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.TimestampCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Optional;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * A {@link TypeCodec} that maps CQL timestamps to Java primitive longs, representing the number of
 * milliseconds since the Epoch.
 *
 * <p>This codec can serve as a replacement for the driver's built-in {@link TypeCodecs#TIMESTAMP
 * timestamp} codec, when application code prefers to deal with raw milliseconds than with {@link
 * Instant} instances.
 *
 * <p>This codec shares its logic with {@link TimestampCodec}. See the javadocs of this codec for
 * important remarks about implementation notes and accepted timestamp formats.
 */
@Immutable
public class TimestampMillisCodec implements PrimitiveLongCodec {

  private final TimestampCodec timestampCodec;

  /**
   * Creates a new {@code TimestampMillisCodec} that uses the system's {@linkplain
   * ZoneId#systemDefault() default time zone} to parse timestamp literals that do not include any
   * time zone information.
   */
  public TimestampMillisCodec() {
    this(ZoneId.systemDefault());
  }

  /**
   * Creates a new {@code TimestampMillisCodec} that uses the given {@link ZoneId} to parse
   * timestamp literals that do not include any time zone information.
   */
  public TimestampMillisCodec(ZoneId defaultZoneId) {
    timestampCodec = new TimestampCodec(defaultZoneId);
  }

  @NonNull
  @Override
  public GenericType<Long> getJavaType() {
    return GenericType.LONG;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.TIMESTAMP;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == Long.class || javaClass == long.class;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    Objects.requireNonNull(value);
    return value instanceof Long;
  }

  @Nullable
  @Override
  public ByteBuffer encodePrimitive(long value, @NonNull ProtocolVersion protocolVersion) {
    return TypeCodecs.BIGINT.encodePrimitive(value, protocolVersion);
  }

  @Override
  public long decodePrimitive(
      @Nullable ByteBuffer value, @NonNull ProtocolVersion protocolVersion) {
    return TypeCodecs.BIGINT.decodePrimitive(value, protocolVersion);
  }

  @Nullable
  @Override
  public Long parse(@Nullable String value) {
    Instant instant = timestampCodec.parse(value);
    return instant == null ? null : instant.toEpochMilli();
  }

  @NonNull
  @Override
  public String format(@Nullable Long value) {
    Instant instant = value == null ? null : Instant.ofEpochMilli(value);
    return timestampCodec.format(instant);
  }

  @NonNull
  @Override
  public Optional<Integer> serializedSize() {
    return Optional.of(8);
  }
}
