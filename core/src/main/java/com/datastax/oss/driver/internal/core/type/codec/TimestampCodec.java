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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.driver.shaded.guava.common.base.Optional;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;
import net.jcip.annotations.ThreadSafe;

/**
 * A codec that handles Apache Cassandra(R)'s timestamp type and maps it to Java's {@link Instant}.
 *
 * <p>Implementation notes:
 *
 * <ol>
 *   <li>Because {@code Instant} uses a precision of nanoseconds, whereas the timestamp type uses a
 *       precision of milliseconds, truncation will happen for any excess precision information as
 *       though the amount in nanoseconds was subject to integer division by one million.
 *   <li>For compatibility reasons, this codec uses the legacy {@link SimpleDateFormat} API
 *       internally when parsing and formatting, and converts from {@link Instant} to {@link Date}
 *       and vice versa. Specially when parsing, this may yield different results as compared to
 *       what the newer Java Time API parsers would have produced for the same input.
 *   <li>Also, {@code Instant} can store points on the time-line further in the future and further
 *       in the past than {@code Date}. This codec will throw an exception when attempting to parse
 *       or format an {@code Instant} falling in this category.
 * </ol>
 *
 * <h3>Accepted date-time formats</h3>
 *
 * The following patterns are valid CQL timestamp literal formats for Apache Cassandra(R) 3.0 and
 * higher, and are thus all recognized when parsing:
 *
 * <ol>
 *   <li>{@code yyyy-MM-dd'T'HH:mm}
 *   <li>{@code yyyy-MM-dd'T'HH:mm:ss}
 *   <li>{@code yyyy-MM-dd'T'HH:mm:ss.SSS}
 *   <li>{@code yyyy-MM-dd'T'HH:mmX}
 *   <li>{@code yyyy-MM-dd'T'HH:mmXX}
 *   <li>{@code yyyy-MM-dd'T'HH:mmXXX}
 *   <li>{@code yyyy-MM-dd'T'HH:mm:ssX}
 *   <li>{@code yyyy-MM-dd'T'HH:mm:ssXX}
 *   <li>{@code yyyy-MM-dd'T'HH:mm:ssXXX}
 *   <li>{@code yyyy-MM-dd'T'HH:mm:ss.SSSX}
 *   <li>{@code yyyy-MM-dd'T'HH:mm:ss.SSSXX}
 *   <li>{@code yyyy-MM-dd'T'HH:mm:ss.SSSXXX}
 *   <li>{@code yyyy-MM-dd'T'HH:mm z}
 *   <li>{@code yyyy-MM-dd'T'HH:mm:ss z}
 *   <li>{@code yyyy-MM-dd'T'HH:mm:ss.SSS z}
 *   <li>{@code yyyy-MM-dd HH:mm}
 *   <li>{@code yyyy-MM-dd HH:mm:ss}
 *   <li>{@code yyyy-MM-dd HH:mm:ss.SSS}
 *   <li>{@code yyyy-MM-dd HH:mmX}
 *   <li>{@code yyyy-MM-dd HH:mmXX}
 *   <li>{@code yyyy-MM-dd HH:mmXXX}
 *   <li>{@code yyyy-MM-dd HH:mm:ssX}
 *   <li>{@code yyyy-MM-dd HH:mm:ssXX}
 *   <li>{@code yyyy-MM-dd HH:mm:ssXXX}
 *   <li>{@code yyyy-MM-dd HH:mm:ss.SSSX}
 *   <li>{@code yyyy-MM-dd HH:mm:ss.SSSXX}
 *   <li>{@code yyyy-MM-dd HH:mm:ss.SSSXXX}
 *   <li>{@code yyyy-MM-dd HH:mm z}
 *   <li>{@code yyyy-MM-dd HH:mm:ss z}
 *   <li>{@code yyyy-MM-dd HH:mm:ss.SSS z}
 *   <li>{@code yyyy-MM-dd}
 *   <li>{@code yyyy-MM-ddX}
 *   <li>{@code yyyy-MM-ddXX}
 *   <li>{@code yyyy-MM-ddXXX}
 *   <li>{@code yyyy-MM-dd z}
 * </ol>
 *
 * By default, when parsing, timestamp literals that do not include any time zone information will
 * be interpreted using the system's {@linkplain ZoneId#systemDefault() default time zone}. This is
 * intended to mimic Apache Cassandra(R)'s own parsing behavior (see {@code
 * org.apache.cassandra.serializers.TimestampSerializer}). The default time zone can be modified
 * using the {@linkplain TimestampCodec#TimestampCodec(ZoneId) one-arg constructor} that takes a
 * custom {@link ZoneId} as an argument.
 *
 * <p>When formatting, the pattern used is always {@code yyyy-MM-dd'T'HH:mm:ss.SSSXXX} and the time
 * zone is either the the system's default one, or the one that was provided when instantiating the
 * codec.
 */
@ThreadSafe
public class TimestampCodec implements TypeCodec<Instant> {

  /**
   * Patterns accepted by Apache Cassandra(R) 3.0 and higher when parsing CQL literals.
   *
   * <p>Note that Cassandra's TimestampSerializer declares many more patterns but some of them are
   * equivalent when parsing.
   */
  private static final String[] DATE_STRING_PATTERNS =
      new String[] {
        // 1) date-time patterns separated by 'T'
        // (declared first because none of the others are ISO compliant, but some of these are)
        // 1.a) without time zone
        "yyyy-MM-dd'T'HH:mm",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        // 1.b) with ISO-8601 time zone
        "yyyy-MM-dd'T'HH:mmX",
        "yyyy-MM-dd'T'HH:mmXX",
        "yyyy-MM-dd'T'HH:mmXXX",
        "yyyy-MM-dd'T'HH:mm:ssX",
        "yyyy-MM-dd'T'HH:mm:ssXX",
        "yyyy-MM-dd'T'HH:mm:ssXXX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        // 1.c) with generic time zone
        "yyyy-MM-dd'T'HH:mm z",
        "yyyy-MM-dd'T'HH:mm:ss z",
        "yyyy-MM-dd'T'HH:mm:ss.SSS z",
        // 2) date-time patterns separated by whitespace
        // 2.a) without time zone
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss.SSS",
        // 2.b) with ISO-8601 time zone
        "yyyy-MM-dd HH:mmX",
        "yyyy-MM-dd HH:mmXX",
        "yyyy-MM-dd HH:mmXXX",
        "yyyy-MM-dd HH:mm:ssX",
        "yyyy-MM-dd HH:mm:ssXX",
        "yyyy-MM-dd HH:mm:ssXXX",
        "yyyy-MM-dd HH:mm:ss.SSSX",
        "yyyy-MM-dd HH:mm:ss.SSSXX",
        "yyyy-MM-dd HH:mm:ss.SSSXXX",
        // 2.c) with generic time zone
        "yyyy-MM-dd HH:mm z",
        "yyyy-MM-dd HH:mm:ss z",
        "yyyy-MM-dd HH:mm:ss.SSS z",
        // 3) date patterns without time
        // 3.a) without time zone
        "yyyy-MM-dd",
        // 3.b) with ISO-8601 time zone
        "yyyy-MM-ddX",
        "yyyy-MM-ddXX",
        "yyyy-MM-ddXXX",
        // 3.c) with generic time zone
        "yyyy-MM-dd z"
      };

  private final FastThreadLocal<SimpleDateFormat> parser;

  private final FastThreadLocal<SimpleDateFormat> formatter;

  /**
   * Creates a new {@code TimestampCodec} that uses the system's {@linkplain ZoneId#systemDefault()
   * default time zone} to parse timestamp literals that do not include any time zone information.
   */
  public TimestampCodec() {
    this(ZoneId.systemDefault());
  }

  /**
   * Creates a new {@code TimestampCodec} that uses the given {@link ZoneId} to parse timestamp
   * literals that do not include any time zone information.
   */
  public TimestampCodec(ZoneId defaultZoneId) {
    parser =
        new FastThreadLocal<SimpleDateFormat>() {
          @Override
          protected SimpleDateFormat initialValue() {
            SimpleDateFormat parser = new SimpleDateFormat();
            parser.setLenient(false);
            parser.setTimeZone(TimeZone.getTimeZone(defaultZoneId));
            return parser;
          }
        };
    formatter =
        new FastThreadLocal<SimpleDateFormat>() {
          @Override
          protected SimpleDateFormat initialValue() {
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            parser.setTimeZone(TimeZone.getTimeZone(defaultZoneId));
            return parser;
          }
        };
  }

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
    return (value == null) ? "NULL" : Strings.quote(formatter.get().format(Date.from(value)));
  }

  @Nullable
  @Override
  public Instant parse(@Nullable String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }
    String unquoted = Strings.unquote(value);
    if (Strings.isLongLiteral(unquoted)) {
      // Numeric literals may be quoted or not
      try {
        return Instant.ofEpochMilli(Long.parseLong(unquoted));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format("Cannot parse timestamp value from \"%s\"", value));
      }
    } else {
      // Alphanumeric literals must be quoted
      if (!Strings.isQuoted(value)) {
        throw new IllegalArgumentException(
            String.format("Alphanumeric timestamp literal must be quoted: \"%s\"", value));
      }
      SimpleDateFormat parser = this.parser.get();
      TimeZone timeZone = parser.getTimeZone();
      ParsePosition pos = new ParsePosition(0);
      for (String pattern : DATE_STRING_PATTERNS) {
        parser.applyPattern(pattern);
        pos.setIndex(0);
        try {
          Date date = parser.parse(unquoted, pos);
          if (date != null && pos.getIndex() == unquoted.length()) {
            return date.toInstant();
          }
        } finally {
          // restore the parser's default time zone, it might have been modified by the call to
          // parse()
          parser.setTimeZone(timeZone);
        }
      }
      throw new IllegalArgumentException(
          String.format("Cannot parse timestamp value from \"%s\"", value));
    }
  }

  @NonNull
  @Override
  public Optional<Integer> serializedSize() {
    return Optional.of(8);
  }
}
