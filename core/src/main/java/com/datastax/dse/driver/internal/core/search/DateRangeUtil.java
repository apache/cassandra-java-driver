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
package com.datastax.dse.driver.internal.core.search;

import com.datastax.dse.driver.api.core.data.time.DateRangePrecision;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

public class DateRangeUtil {

  /** Sets all the fields smaller than the given unit to their lowest possible value. */
  @NonNull
  public static ZonedDateTime roundDown(@NonNull ZonedDateTime date, @NonNull ChronoUnit unit) {
    switch (unit) {
      case YEARS:
        return date.with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS);
      case MONTHS:
        return date.with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS);
      case DAYS:
      case HOURS:
      case MINUTES:
      case SECONDS:
      case MILLIS:
        return date.truncatedTo(unit);
      default:
        throw new IllegalArgumentException("Unsupported unit for rounding: " + unit);
    }
  }

  /** Sets all the fields smaller than the given unit to their highest possible value. */
  @NonNull
  public static ZonedDateTime roundUp(@NonNull ZonedDateTime date, @NonNull ChronoUnit unit) {
    return roundDown(date, unit)
        .plus(1, unit)
        // Even though ZDT has nanosecond-precision, DSE only rounds to millisecond precision so be
        // consistent with that
        .minus(1, ChronoUnit.MILLIS);
  }

  /**
   * Parses the given string as a date in a range bound.
   *
   * <p>This method deliberately uses legacy time APIs, in order to be as close as possible to the
   * server-side parsing logic. We want the client to behave exactly like the server, i.e. parsing a
   * date locally and inlining it in a CQL query should always yield the same result as binding the
   * date as a value.
   */
  public static Calendar parseCalendar(String source) throws ParseException {
    // The contents of this method are based on Lucene's DateRangePrefixTree#parseCalendar, released
    // under the Apache License, Version 2.0.
    // Following is the original notice from that file:

    // Licensed to the Apache Software Foundation (ASF) under one or more
    // contributor license agreements.  See the NOTICE file distributed with
    // this work for additional information regarding copyright ownership.
    // The ASF licenses this file to You under the Apache License, Version 2.0
    // (the "License"); you may not use this file except in compliance with
    // the License.  You may obtain a copy of the License at
    //
    //     http://www.apache.org/licenses/LICENSE-2.0
    //
    // Unless required by applicable law or agreed to in writing, software
    // distributed under the License is distributed on an "AS IS" BASIS,
    // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    // See the License for the specific language governing permissions and
    // limitations under the License.

    if (source == null || source.isEmpty()) {
      throw new IllegalArgumentException("Can't parse a null or blank string");
    }

    Calendar calendar = newCalendar();
    if (source.equals("*")) {
      return calendar;
    }
    int offset = 0; // a pointer
    try {
      // year & era:
      int lastOffset =
          (source.charAt(source.length() - 1) == 'Z') ? source.length() - 1 : source.length();
      int hyphenIdx = source.indexOf('-', 1); // look past possible leading hyphen
      if (hyphenIdx < 0) {
        hyphenIdx = lastOffset;
      }
      int year = Integer.parseInt(source.substring(offset, hyphenIdx));
      calendar.set(Calendar.ERA, year <= 0 ? 0 : 1);
      calendar.set(Calendar.YEAR, year <= 0 ? -1 * year + 1 : year);
      offset = hyphenIdx + 1;
      if (lastOffset < offset) {
        return calendar;
      }

      // NOTE: We aren't validating separator chars, and we unintentionally accept leading +/-.
      // The str.substring()'s hopefully get optimized to be stack-allocated.

      // month:
      calendar.set(
          Calendar.MONTH,
          Integer.parseInt(source.substring(offset, offset + 2)) - 1); // starts at 0
      offset += 3;
      if (lastOffset < offset) {
        return calendar;
      }
      // day:
      calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(source.substring(offset, offset + 2)));
      offset += 3;
      if (lastOffset < offset) {
        return calendar;
      }
      // hour:
      calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(source.substring(offset, offset + 2)));
      offset += 3;
      if (lastOffset < offset) {
        return calendar;
      }
      // minute:
      calendar.set(Calendar.MINUTE, Integer.parseInt(source.substring(offset, offset + 2)));
      offset += 3;
      if (lastOffset < offset) {
        return calendar;
      }
      // second:
      calendar.set(Calendar.SECOND, Integer.parseInt(source.substring(offset, offset + 2)));
      offset += 3;
      if (lastOffset < offset) {
        return calendar;
      }
      // ms:
      calendar.set(Calendar.MILLISECOND, Integer.parseInt(source.substring(offset, offset + 3)));
      offset += 3; // last one, move to next char
      if (lastOffset == offset) {
        return calendar;
      }
    } catch (Exception e) {
      ParseException pe = new ParseException("Improperly formatted date: " + source, offset);
      pe.initCause(e);
      throw pe;
    }
    throw new ParseException("Improperly formatted date: " + source, offset);
  }

  private static Calendar newCalendar() {
    Calendar calendar = Calendar.getInstance(UTC, Locale.ROOT);
    calendar.clear();
    return calendar;
  }

  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  /**
   * Returns the precision of a calendar obtained through {@link #parseCalendar(String)}, or {@code
   * null} if no field was set.
   */
  @Nullable
  public static DateRangePrecision getPrecision(Calendar calendar) {
    DateRangePrecision lastPrecision = null;
    for (Map.Entry<DateRangePrecision, Integer> entry : FIELD_BY_PRECISION.entrySet()) {
      DateRangePrecision precision = entry.getKey();
      int field = entry.getValue();
      if (calendar.isSet(field)) {
        lastPrecision = precision;
      } else {
        break;
      }
    }
    return lastPrecision;
  }

  // Note: this could be a field on DateRangePrecision, but it's only used within this class so it's
  // better not to expose it.
  private static final ImmutableMap<DateRangePrecision, Integer> FIELD_BY_PRECISION =
      ImmutableMap.<DateRangePrecision, Integer>builder()
          .put(DateRangePrecision.YEAR, Calendar.YEAR)
          .put(DateRangePrecision.MONTH, Calendar.MONTH)
          .put(DateRangePrecision.DAY, Calendar.DAY_OF_MONTH)
          .put(DateRangePrecision.HOUR, Calendar.HOUR_OF_DAY)
          .put(DateRangePrecision.MINUTE, Calendar.MINUTE)
          .put(DateRangePrecision.SECOND, Calendar.SECOND)
          .put(DateRangePrecision.MILLISECOND, Calendar.MILLISECOND)
          .build();

  public static ZonedDateTime toZonedDateTime(Calendar calendar) {
    int year = calendar.get(Calendar.YEAR);
    if (calendar.get(Calendar.ERA) == 0) {
      // BC era; 1 BC == 0 AD, 0 BD == -1 AD, etc
      year -= 1;
      if (year > 0) {
        year = -year;
      }
    }
    LocalDateTime localDateTime =
        LocalDateTime.of(
            year,
            calendar.get(Calendar.MONTH) + 1,
            calendar.get(Calendar.DAY_OF_MONTH),
            calendar.get(Calendar.HOUR_OF_DAY),
            calendar.get(Calendar.MINUTE),
            calendar.get(Calendar.SECOND));
    localDateTime =
        localDateTime.with(ChronoField.MILLI_OF_SECOND, calendar.get(Calendar.MILLISECOND));
    return ZonedDateTime.of(localDateTime, ZoneOffset.UTC);
  }
}
