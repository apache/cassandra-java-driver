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

import static java.lang.Long.parseLong;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.Strings;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class TimestampCodec implements TypeCodec<Instant> {

  /** A {@link DateTimeFormatter} that parses (most) of the ISO formats accepted in CQL. */
  private static final DateTimeFormatter PARSER =
      new java.time.format.DateTimeFormatterBuilder()
          .parseCaseSensitive()
          .parseStrict()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .optionalStart()
          .appendLiteral('T')
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .optionalEnd()
          .optionalStart()
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .optionalEnd()
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .optionalEnd()
          .optionalStart()
          .appendZoneId()
          .optionalEnd()
          .toFormatter()
          .withZone(ZoneOffset.UTC);

  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx").withZone(ZoneOffset.UTC);

  @NonNull
  @Override
  public GenericType<Instant> getJavaType() {
    return GenericType.INSTANT;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.TIMESTAMP;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof Instant;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == Instant.class;
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable Instant value, @NonNull ProtocolVersion protocolVersion) {
    return (value == null)
        ? null
        : TypeCodecs.BIGINT.encodePrimitive(value.toEpochMilli(), protocolVersion);
  }

  @Nullable
  @Override
  public Instant decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    return (bytes == null || bytes.remaining() == 0)
        ? null
        : Instant.ofEpochMilli(TypeCodecs.BIGINT.decodePrimitive(bytes, protocolVersion));
  }

  @NonNull
  @Override
  public String format(@Nullable Instant value) {
    return (value == null) ? "NULL" : Strings.quote(FORMATTER.format(value));
  }

  @Nullable
  @Override
  public Instant parse(@Nullable String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }
    // strip enclosing single quotes, if any
    if (Strings.isQuoted(value)) {
      value = Strings.unquote(value);
    }
    if (Strings.isLongLiteral(value)) {
      try {
        return Instant.ofEpochMilli(parseLong(value));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format("Cannot parse timestamp value from \"%s\"", value));
      }
    }
    try {
      return Instant.from(PARSER.parse(value));
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          String.format("Cannot parse timestamp value from \"%s\"", value));
    }
  }
}
