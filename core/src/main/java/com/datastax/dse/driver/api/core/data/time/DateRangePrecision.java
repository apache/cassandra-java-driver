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
package com.datastax.dse.driver.api.core.data.time;

import com.datastax.dse.driver.internal.core.search.DateRangeUtil;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;

/** The precision of a {@link DateRangeBound}. */
public enum DateRangePrecision {
  MILLISECOND(
      0x06,
      ChronoUnit.MILLIS,
      new DateTimeFormatterBuilder()
          .parseCaseSensitive()
          .parseStrict()
          .appendPattern("uuuu-MM-dd'T'HH:mm:ss.SSS")
          .optionalStart()
          .appendZoneId()
          .optionalEnd()
          .toFormatter()
          .withZone(ZoneOffset.UTC)
          .withLocale(Locale.ROOT)),
  SECOND(
      0x05,
      ChronoUnit.SECONDS,
      new DateTimeFormatterBuilder()
          .parseCaseSensitive()
          .parseStrict()
          .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
          .parseDefaulting(ChronoField.MILLI_OF_SECOND, 0)
          .toFormatter()
          .withZone(ZoneOffset.UTC)
          .withLocale(Locale.ROOT)),
  MINUTE(
      0x04,
      ChronoUnit.MINUTES,
      new DateTimeFormatterBuilder()
          .parseCaseSensitive()
          .parseStrict()
          .appendPattern("uuuu-MM-dd'T'HH:mm")
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .parseDefaulting(ChronoField.MILLI_OF_SECOND, 0)
          .toFormatter()
          .withZone(ZoneOffset.UTC)
          .withLocale(Locale.ROOT)),
  HOUR(
      0x03,
      ChronoUnit.HOURS,
      new DateTimeFormatterBuilder()
          .parseCaseSensitive()
          .parseStrict()
          .appendPattern("uuuu-MM-dd'T'HH")
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .parseDefaulting(ChronoField.MILLI_OF_SECOND, 0)
          .toFormatter()
          .withZone(ZoneOffset.UTC)
          .withLocale(Locale.ROOT)),
  DAY(
      0x02,
      ChronoUnit.DAYS,
      new DateTimeFormatterBuilder()
          .parseCaseSensitive()
          .parseStrict()
          .appendPattern("uuuu-MM-dd")
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .parseDefaulting(ChronoField.MILLI_OF_SECOND, 0)
          .toFormatter()
          .withZone(ZoneOffset.UTC)
          .withLocale(Locale.ROOT)),
  MONTH(
      0x01,
      ChronoUnit.MONTHS,
      new DateTimeFormatterBuilder()
          .parseCaseSensitive()
          .parseStrict()
          .appendPattern("uuuu-MM")
          .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .parseDefaulting(ChronoField.MILLI_OF_SECOND, 0)
          .toFormatter()
          .withZone(ZoneOffset.UTC)
          .withLocale(Locale.ROOT)),
  YEAR(
      0x00,
      ChronoUnit.YEARS,
      new DateTimeFormatterBuilder()
          .parseCaseSensitive()
          .parseStrict()
          .appendPattern("uuuu")
          .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
          .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .parseDefaulting(ChronoField.MILLI_OF_SECOND, 0)
          .toFormatter()
          .withZone(ZoneOffset.UTC)
          .withLocale(Locale.ROOT));

  private final byte encoding;
  private final ChronoUnit roundingUnit;
  // The formatter is only used for formatting (parsing is done with  DateRangeUtil.parseCalendar to
  // be exactly the same as DSE's).
  // If that ever were to change, note that DateTimeFormatters with a time zone have a parsing bug
  // in Java 8: the formatter's zone will always be used, even if the input string specifies one
  // explicitly.
  // See https://stackoverflow.com/questions/41999421
  private final DateTimeFormatter formatter;

  DateRangePrecision(int encoding, ChronoUnit roundingUnit, DateTimeFormatter formatter) {
    this.encoding = (byte) encoding;
    this.roundingUnit = roundingUnit;
    this.formatter = formatter;
  }

  private static final Map<Byte, DateRangePrecision> ENCODINGS;

  static {
    ImmutableMap.Builder<Byte, DateRangePrecision> builder = ImmutableMap.builder();
    for (DateRangePrecision precision : values()) {
      builder.put(precision.encoding, precision);
    }
    ENCODINGS = builder.build();
  }

  public static DateRangePrecision fromEncoding(byte encoding) {
    DateRangePrecision precision = ENCODINGS.get(encoding);
    if (precision == null) {
      throw new IllegalArgumentException("Invalid precision encoding: " + encoding);
    }
    return precision;
  }

  /** The code used to represent the precision when a date range is encoded to binary. */
  public byte getEncoding() {
    return encoding;
  }

  /**
   * Rounds up the given timestamp to this precision.
   *
   * <p>Temporal fields smaller than this precision will be rounded up; other fields will be left
   * untouched.
   */
  @NonNull
  public ZonedDateTime roundUp(@NonNull ZonedDateTime timestamp) {
    Preconditions.checkNotNull(timestamp);
    return DateRangeUtil.roundUp(timestamp, roundingUnit);
  }

  /**
   * Rounds down the given timestamp to this precision.
   *
   * <p>Temporal fields smaller than this precision will be rounded down; other fields will be left
   * untouched.
   */
  @NonNull
  public ZonedDateTime roundDown(@NonNull ZonedDateTime timestamp) {
    Preconditions.checkNotNull(timestamp);
    return DateRangeUtil.roundDown(timestamp, roundingUnit);
  }

  /** Formats the given timestamp according to this precision. */
  public String format(ZonedDateTime timestamp) {
    return formatter.format(timestamp);
  }
}
