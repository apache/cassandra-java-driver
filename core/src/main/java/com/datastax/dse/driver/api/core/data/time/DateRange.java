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

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Serializable;
import java.text.ParseException;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;

/**
 * A date range, as defined by the server type {@code
 * org.apache.cassandra.db.marshal.DateRangeType}, corresponding to the Apache Solr type <a
 * href="https://lucene.apache.org/solr/6_3_0/solr-core/index.html?org/apache/solr/schema/DateRangeField.html">{@code
 * DateRangeField}</a>.
 *
 * <p>A date range can be either {@linkplain DateRange#DateRange(DateRangeBound) single-bounded}, in
 * which case it represents a unique instant (e.g. "{@code 2001-01-01}"), or {@linkplain
 * #DateRange(DateRangeBound, DateRangeBound) double-bounded}, in which case it represents an
 * interval of time (e.g. "{@code [2001-01-01 TO 2002]}").
 *
 * <p>Date range {@linkplain DateRangeBound bounds} are always inclusive; they must be either valid
 * dates, or the special value {@link DateRangeBound#UNBOUNDED UNBOUNDED}, represented by a "{@code
 * *}", e.g. "{@code [2001 TO *]}".
 *
 * <p>Instances can be more easily created with the {@link #parse(String)} method.
 *
 * <p>This class is immutable and thread-safe.
 *
 * @since DSE 5.1
 */
public class DateRange implements Serializable {

  /**
   * Parses the given string as a date range.
   *
   * <p>The given input must be compliant with Apache Solr type <a
   * href="https://lucene.apache.org/solr/6_3_0/solr-core/index.html?org/apache/solr/schema/DateRangeField.html">{@code
   * DateRangeField}</a> syntax; it can either be a {@linkplain #DateRange(DateRangeBound)
   * single-bounded range}, or a {@linkplain #DateRange(DateRangeBound, DateRangeBound)
   * double-bounded range}.
   *
   * @throws ParseException if the given string could not be parsed into a valid range.
   * @see DateRangeBound#parseLowerBound(String)
   * @see DateRangeBound#parseUpperBound(String)
   */
  @NonNull
  public static DateRange parse(@NonNull String source) throws ParseException {
    if (Strings.isNullOrEmpty(source)) {
      throw new ParseException("Date range is null or empty", 0);
    }

    if (source.charAt(0) == '[') {
      if (source.charAt(source.length() - 1) != ']') {
        throw new ParseException(
            "If date range starts with '[' it must end with ']'; got " + source,
            source.length() - 1);
      }
      int middle = source.indexOf(" TO ");
      if (middle < 0) {
        throw new ParseException(
            "If date range starts with '[' it must contain ' TO '; got " + source, 0);
      }
      String lowerBoundString = source.substring(1, middle);
      int upperBoundStart = middle + 4;
      String upperBoundString = source.substring(upperBoundStart, source.length() - 1);
      DateRangeBound lowerBound;
      try {
        lowerBound = DateRangeBound.parseLowerBound(lowerBoundString);
      } catch (Exception e) {
        throw newParseException("Cannot parse date range lower bound: " + source, 1, e);
      }
      DateRangeBound upperBound;
      try {
        upperBound = DateRangeBound.parseUpperBound(upperBoundString);
      } catch (Exception e) {
        throw newParseException(
            "Cannot parse date range upper bound: " + source, upperBoundStart, e);
      }
      return new DateRange(lowerBound, upperBound);
    } else {
      try {
        return new DateRange(DateRangeBound.parseLowerBound(source));
      } catch (Exception e) {
        throw newParseException("Cannot parse single date range bound: " + source, 0, e);
      }
    }
  }

  @NonNull private final DateRangeBound lowerBound;
  @Nullable private final DateRangeBound upperBound;

  /**
   * Creates a "single bounded" instance, i.e., a date range whose upper and lower bounds are
   * identical.
   *
   * @throws NullPointerException if {@code singleBound} is null.
   */
  public DateRange(@NonNull DateRangeBound singleBound) {
    this.lowerBound = Preconditions.checkNotNull(singleBound, "singleBound cannot be null");
    this.upperBound = null;
  }

  /**
   * Creates an instance composed of two distinct bounds.
   *
   * @throws NullPointerException if {@code lowerBound} or {@code upperBound} is null.
   * @throws IllegalArgumentException if both {@code lowerBound} and {@code upperBound} are not
   *     unbounded and {@code lowerBound} is greater than {@code upperBound}.
   */
  public DateRange(@NonNull DateRangeBound lowerBound, @NonNull DateRangeBound upperBound) {
    Preconditions.checkNotNull(lowerBound, "lowerBound cannot be null");
    Preconditions.checkNotNull(upperBound, "upperBound cannot be null");
    if (!lowerBound.isUnbounded()
        && !upperBound.isUnbounded()
        && lowerBound.getTimestamp().compareTo(upperBound.getTimestamp()) >= 0) {
      throw new IllegalArgumentException(
          String.format(
              "Lower bound of a date range should be before upper bound, got: [%s TO %s]",
              lowerBound, upperBound));
    }
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  /** Returns the lower bound of this range (inclusive). */
  @NonNull
  public DateRangeBound getLowerBound() {
    return lowerBound;
  }

  /**
   * Returns the upper bound of this range (inclusive), or empty if the range is {@linkplain
   * #isSingleBounded() single-bounded}.
   */
  @NonNull
  public Optional<DateRangeBound> getUpperBound() {
    return Optional.ofNullable(upperBound);
  }

  /**
   * Returns whether this range is single-bounded, i.e. if the upper and lower bounds are identical.
   */
  public boolean isSingleBounded() {
    return upperBound == null;
  }

  /**
   * Returns the string representation of this range, in a format compatible with <a
   * href="https://cwiki.apache.org/confluence/display/solr/Working+with+Dates">Apache Solr
   * DateRageField syntax</a>
   *
   * @see DateRangeBound#toString()
   */
  @NonNull
  @Override
  public String toString() {
    if (isSingleBounded()) {
      return lowerBound.toString();
    } else {
      return String.format("[%s TO %s]", lowerBound, upperBound);
    }
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DateRange) {
      DateRange that = (DateRange) other;
      return Objects.equals(this.lowerBound, that.lowerBound)
          && Objects.equals(this.upperBound, that.upperBound);

    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(lowerBound, upperBound);
  }

  private static ParseException newParseException(String message, int offset, Exception cause) {
    ParseException parseException = new ParseException(message, offset);
    parseException.initCause(cause);
    return parseException;
  }

  /**
   * This object gets replaced by an internal proxy for serialization.
   *
   * @serialData the lower bound timestamp and precision, followed by the upper bound timestamp and
   *     precision, or two {@code null}s if the range is single-bounded.
   */
  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private static class SerializationProxy implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ZonedDateTime lowerBoundTimestamp;
    private final DateRangePrecision lowerBoundPrecision;
    private final ZonedDateTime upperBoundTimestamp;
    private final DateRangePrecision upperBoundPrecision;

    SerializationProxy(DateRange input) {
      this.lowerBoundTimestamp = input.lowerBound.getTimestamp();
      this.lowerBoundPrecision = input.lowerBound.getPrecision();
      if (input.upperBound != null) {
        this.upperBoundTimestamp = input.upperBound.getTimestamp();
        this.upperBoundPrecision = input.upperBound.getPrecision();
      } else {
        this.upperBoundTimestamp = null;
        this.upperBoundPrecision = null;
      }
    }

    private Object readResolve() {
      if (upperBoundTimestamp == null ^ upperBoundPrecision == null) {
        // Should not happen, but protect against corrupted streams
        throw new IllegalArgumentException(
            "Invalid serialized form, upper bound timestamp and precision "
                + "should be either both null or both non-null");
      }

      if (upperBoundTimestamp == null) {
        return new DateRange(DateRangeBound.lowerBound(lowerBoundTimestamp, lowerBoundPrecision));
      } else {
        return new DateRange(
            DateRangeBound.lowerBound(lowerBoundTimestamp, lowerBoundPrecision),
            DateRangeBound.upperBound(upperBoundTimestamp, upperBoundPrecision));
      }
    }
  }
}
