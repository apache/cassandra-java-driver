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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import net.jcip.annotations.ThreadSafe;

/**
 * A codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link
 * ZonedDateTime}, using the {@link ZoneId} supplied at instantiation.
 *
 * <p>Note that Apache Cassandra(R)'s timestamp type does not store any time zone; this codec is
 * provided merely as a convenience for users that need to deal with zoned timestamps in their
 * applications.
 *
 * <p>This codec shares its logic with {@link TimestampCodec}. See the javadocs of this codec for
 * important remarks about implementation notes and accepted timestamp formats.
 *
 * @see TimestampCodec
 */
@ThreadSafe
public class ZonedTimestampCodec implements TypeCodec<ZonedDateTime> {

  private final TypeCodec<Instant> instantCodec;
  private final ZoneId timeZone;

  /**
   * Creates a new {@code ZonedTimestampCodec} that converts CQL timestamps into {@link
   * ZonedDateTime} instances using the system's {@linkplain ZoneId#systemDefault() default time
   * zone} as their time zone. The supplied {@code timeZone} will also be used to parse CQL
   * timestamp literals that do not include any time zone information.
   */
  public ZonedTimestampCodec() {
    this(ZoneId.systemDefault());
  }

  /**
   * Creates a new {@code ZonedTimestampCodec} that converts CQL timestamps into {@link
   * ZonedDateTime} instances using the given {@link ZoneId} as their time zone. The supplied {@code
   * timeZone} will also be used to parse CQL timestamp literals that do not include any time zone
   * information.
   */
  public ZonedTimestampCodec(ZoneId timeZone) {
    instantCodec = new TimestampCodec(timeZone);
    this.timeZone = timeZone;
  }

  @NonNull
  @Override
  public GenericType<ZonedDateTime> getJavaType() {
    return GenericType.ZONED_DATE_TIME;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.TIMESTAMP;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof ZonedDateTime;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == ZonedDateTime.class;
  }

  @Nullable
  @Override
  public ByteBuffer encode(
      @Nullable ZonedDateTime value, @NonNull ProtocolVersion protocolVersion) {
    return instantCodec.encode(value != null ? value.toInstant() : null, protocolVersion);
  }

  @Nullable
  @Override
  public ZonedDateTime decode(
      @Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    Instant instant = instantCodec.decode(bytes, protocolVersion);
    if (instant == null) {
      return null;
    }
    return instant.atZone(timeZone);
  }

  @NonNull
  @Override
  public String format(@Nullable ZonedDateTime value) {
    return instantCodec.format(value != null ? value.toInstant() : null);
  }

  @Nullable
  @Override
  public ZonedDateTime parse(@Nullable String value) {
    Instant instant = instantCodec.parse(value);
    if (instant == null) {
      return null;
    }
    return instant.atZone(timeZone);
  }
}
