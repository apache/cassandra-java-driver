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
package com.datastax.driver.extras.codecs.jdk8;

import static com.datastax.driver.core.ParseUtils.isLongLiteral;
import static com.datastax.driver.core.ParseUtils.quote;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.IgnoreJDK6Requirement;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.nio.ByteBuffer;

/**
 * {@link TypeCodec} that maps {@link java.time.LocalDateTime} to CQL {@code timestamp}, allowing
 * the setting and retrieval of {@code timestamp} columns as {@link java.time.LocalDateTime}
 * instances.
 *
 * <p><strong>IMPORTANT</strong>
 *
 * <p>1) The default timestamp formatter used by this codec produces CQL literals that may include
 * milliseconds. <strong>This literal format is incompatible with Cassandra < 2.0.9.</strong>
 *
 * <p>2) Even if the ISO-8601 standard accepts timestamps with nanosecond precision, Cassandra
 * timestamps have millisecond precision; therefore, any sub-millisecond value set on a {@link
 * java.time.LocalDateTime} will be lost when persisted to Cassandra.
 *
 * @see <a href="https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtimestamps">'Working with
 *     timestamps' section of CQL specification</a>
 */
@IgnoreJDK6Requirement
@SuppressWarnings("Since15")
public class LocalDateTimeCodec extends TypeCodec<java.time.LocalDateTime> {

  private static final java.time.format.DateTimeFormatter FORMATTER =
      java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  public static final LocalDateTimeCodec instance = new LocalDateTimeCodec();

  private LocalDateTimeCodec() {
    super(DataType.timestamp(), java.time.LocalDateTime.class);
  }

  @Override
  public ByteBuffer serialize(java.time.LocalDateTime value, ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    long millis = value.atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
    return bigint().serializeNoBoxing(millis, protocolVersion);
  }

  @Override
  public java.time.LocalDateTime deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    }
    long millis = bigint().deserializeNoBoxing(bytes, protocolVersion);
    return java.time.LocalDateTime.ofInstant(
        java.time.Instant.ofEpochMilli(millis), java.time.ZoneOffset.UTC);
  }

  @Override
  public String format(java.time.LocalDateTime value) {
    if (value == null) {
      return "NULL";
    }
    return quote(FORMATTER.format(value));
  }

  @Override
  public java.time.LocalDateTime parse(String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }
    // strip enclosing single quotes, if any
    if (ParseUtils.isQuoted(value)) {
      value = ParseUtils.unquote(value);
    }
    if (isLongLiteral(value)) {
      try {
        long millis = Long.parseLong(value);
        return java.time.LocalDateTime.ofInstant(
            java.time.Instant.ofEpochMilli(millis), java.time.ZoneOffset.UTC);
      } catch (NumberFormatException e) {
        throw new InvalidTypeException(
            String.format("Cannot parse timestamp value from \"%s\"", value));
      }
    }
    try {
      return java.time.LocalDateTime.from(FORMATTER.parse(value));
    } catch (java.time.format.DateTimeParseException e) {
      throw new InvalidTypeException(
          String.format("Cannot parse timestamp value from \"%s\"", value));
    }
  }
}
