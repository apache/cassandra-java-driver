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
package com.datastax.dse.driver.api.core.data.time;

import com.datastax.dse.driver.internal.core.search.DateRangeUtil;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.text.ParseException;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Objects;

/**
 * A date range bound.
 *
 * <p>It is composed of a {@link ZonedDateTime} field and a corresponding {@link
 * DateRangePrecision}.
 *
 * <p>Date range bounds are inclusive. The special value {@link #UNBOUNDED} denotes an un unbounded
 * (infinite) bound, represented by a {@code *} sign.
 *
 * <p>This class is immutable and thread-safe.
 */
public class DateRangeBound {

  /**
   * The unbounded {@link DateRangeBound} instance. It is syntactically represented by a {@code *}
   * (star) sign.
   */
  public static final DateRangeBound UNBOUNDED = new DateRangeBound();

  /**
   * Parses the given input as a lower date range bound.
   *
   * <p>The input should be a <a
   * href="https://github.com/apache/lucene-solr/blob/releases/lucene-solr/6.4.0/lucene/spatial-extras/src/java/org/apache/lucene/spatial/prefix/tree/DateRangePrefixTree.java#L441">Lucene-compliant</a>
   * string.
   *
   * <p>The returned bound will have its {@linkplain DateRangePrecision precision} inferred from the
   * input, and its timestamp will be {@linkplain DateRangePrecision#roundDown(ZonedDateTime)
   * rounded down} to that precision.
   *
   * <p>Note that, in order to align with the server's parsing behavior, dates will always be parsed
   * in the UTC time zone.
   *
   * @throws NullPointerException if {@code lowerBound} is {@code null}.
   * @throws ParseException if the given input cannot be parsed.
   */
  @NonNull
  public static DateRangeBound parseLowerBound(@NonNull String source) throws ParseException {
    Preconditions.checkNotNull(source);
    Calendar calendar = DateRangeUtil.parseCalendar(source);
    DateRangePrecision precision = DateRangeUtil.getPrecision(calendar);
    return (precision == null)
        ? UNBOUNDED
        : lowerBound(DateRangeUtil.toZonedDateTime(calendar), precision);
  }

  /**
   * Parses the given input as an upper date range bound.
   *
   * <p>The input should be a <a
   * href="https://github.com/apache/lucene-solr/blob/releases/lucene-solr/6.4.0/lucene/spatial-extras/src/java/org/apache/lucene/spatial/prefix/tree/DateRangePrefixTree.java#L441">Lucene-compliant</a>
   * string.
   *
   * <p>The returned bound will have its {@linkplain DateRangePrecision precision} inferred from the
   * input, and its timestamp will be {@linkplain DateRangePrecision#roundUp(ZonedDateTime)} rounded
   * up} to that precision.
   *
   * <p>Note that, in order to align with the server's behavior (e.g. when using date range literals
   * in CQL query strings), dates must always be in the UTC time zone: an optional trailing {@code
   * Z}" is allowed, but no other time zone ID (not even {@code UTC}, {@code GMT} or {@code +00:00})
   * is permitted.
   *
   * @throws NullPointerException if {@code upperBound} is {@code null}.
   * @throws ParseException if the given input cannot be parsed.
   */
  public static DateRangeBound parseUpperBound(String source) throws ParseException {
    Preconditions.checkNotNull(source);
    Calendar calendar = DateRangeUtil.parseCalendar(source);
    DateRangePrecision precision = DateRangeUtil.getPrecision(calendar);
    return (precision == null)
        ? UNBOUNDED
        : upperBound(DateRangeUtil.toZonedDateTime(calendar), precision);
  }

  /**
   * Creates a date range lower bound from the given date and precision. Temporal fields smaller
   * than the precision will be rounded down.
   */
  public static DateRangeBound lowerBound(ZonedDateTime timestamp, DateRangePrecision precision) {
    return new DateRangeBound(precision.roundDown(timestamp), precision);
  }

  /**
   * Creates a date range upper bound from the given date and precision. Temporal fields smaller
   * than the precision will be rounded up.
   */
  public static DateRangeBound upperBound(ZonedDateTime timestamp, DateRangePrecision precision) {
    return new DateRangeBound(precision.roundUp(timestamp), precision);
  }

  @Nullable private final ZonedDateTime timestamp;
  @Nullable private final DateRangePrecision precision;

  private DateRangeBound(@NonNull ZonedDateTime timestamp, @NonNull DateRangePrecision precision) {
    Preconditions.checkNotNull(timestamp);
    Preconditions.checkNotNull(precision);
    this.timestamp = timestamp;
    this.precision = precision;
  }

  // constructor used for the special UNBOUNDED value
  private DateRangeBound() {
    this.timestamp = null;
    this.precision = null;
  }

  /** Whether this bound is unbounded (i.e. denotes the special {@code *} value). */
  public boolean isUnbounded() {
    return this.timestamp == null && this.precision == null;
  }

  /**
   * Returns the timestamp of this bound.
   *
   * @throws IllegalStateException if this bound is {@linkplain #isUnbounded() unbounded}.
   */
  @NonNull
  public ZonedDateTime getTimestamp() {
    if (isUnbounded()) {
      throw new IllegalStateException(
          "Can't call this method on UNBOUNDED, use isUnbounded() to check first");
    }
    assert timestamp != null;
    return timestamp;
  }

  /**
   * Returns the precision of this bound.
   *
   * @throws IllegalStateException if this bound is {@linkplain #isUnbounded() unbounded}.
   */
  @NonNull
  public DateRangePrecision getPrecision() {
    if (isUnbounded()) {
      throw new IllegalStateException(
          "Can't call this method on UNBOUNDED, use isUnbounded() to check first");
    }
    assert precision != null;
    return precision;
  }

  /**
   * Returns this bound as a <a
   * href="https://github.com/apache/lucene-solr/blob/releases/lucene-solr/6.4.0/lucene/spatial-extras/src/java/org/apache/lucene/spatial/prefix/tree/DateRangePrefixTree.java#L363"
   * >Lucene-compliant</a> string.
   *
   * <p>Unbounded bounds always return "{@code *}"; all other bounds are formatted in one of the
   * common ISO-8601 datetime formats, depending on their precision.
   *
   * <p>Note that Lucene expects timestamps in UTC only. Timezone presence is always optional, and
   * if present, it must be expressed with the symbol "Z" exclusively. Therefore this method does
   * not include any timezone information in the returned string, except for bounds with {@linkplain
   * DateRangePrecision#MILLISECOND millisecond} precision, where the symbol "Z" is always appended
   * to the resulting string.
   */
  @NonNull
  @Override
  public String toString() {
    if (isUnbounded()) {
      return "*";
    } else {
      assert timestamp != null && precision != null;
      return precision.format(timestamp);
    }
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DateRangeBound) {
      DateRangeBound that = (DateRangeBound) other;
      return Objects.equals(this.timestamp, that.timestamp)
          && Objects.equals(this.precision, that.precision);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, precision);
  }
}
