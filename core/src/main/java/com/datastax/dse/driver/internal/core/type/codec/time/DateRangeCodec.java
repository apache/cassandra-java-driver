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
package com.datastax.dse.driver.internal.core.type.codec.time;

import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.api.core.data.time.DateRangeBound;
import com.datastax.dse.driver.api.core.data.time.DateRangePrecision;
import com.datastax.dse.driver.api.core.type.DseDataTypes;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.Strings;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

public class DateRangeCodec implements TypeCodec<DateRange> {

  private static final GenericType<DateRange> JAVA_TYPE = GenericType.of(DateRange.class);
  private static final DataType CQL_TYPE = DseDataTypes.DATE_RANGE;

  // e.g. [2001-01-01]
  private static final byte DATE_RANGE_TYPE_SINGLE_DATE = 0x00;
  // e.g. [2001-01-01 TO 2001-01-31]
  private static final byte DATE_RANGE_TYPE_CLOSED_RANGE = 0x01;
  // e.g. [2001-01-01 TO *]
  private static final byte DATE_RANGE_TYPE_OPEN_RANGE_HIGH = 0x02;
  // e.g. [* TO 2001-01-01]
  private static final byte DATE_RANGE_TYPE_OPEN_RANGE_LOW = 0x03;
  // [* TO *]
  private static final byte DATE_RANGE_TYPE_BOTH_OPEN_RANGE = 0x04;
  // *
  private static final byte DATE_RANGE_TYPE_SINGLE_DATE_OPEN = 0x05;

  @NonNull
  @Override
  public GenericType<DateRange> getJavaType() {
    return JAVA_TYPE;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return CQL_TYPE;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == DateRange.class;
  }

  @Nullable
  @Override
  public ByteBuffer encode(
      @Nullable DateRange dateRange, @NonNull ProtocolVersion protocolVersion) {
    if (dateRange == null) {
      return null;
    }
    byte rangeType = encodeType(dateRange);
    int bufferSize = 1;
    DateRangeBound lowerBound = dateRange.getLowerBound();
    Optional<DateRangeBound> maybeUpperBound = dateRange.getUpperBound();
    bufferSize += lowerBound.isUnbounded() ? 0 : 9;
    bufferSize += maybeUpperBound.map(upperBound -> upperBound.isUnbounded() ? 0 : 9).orElse(0);
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    buffer.put(rangeType);
    if (!lowerBound.isUnbounded()) {
      put(buffer, lowerBound);
    }
    maybeUpperBound.ifPresent(
        upperBound -> {
          if (!upperBound.isUnbounded()) {
            put(buffer, upperBound);
          }
        });
    return (ByteBuffer) buffer.flip();
  }

  private static byte encodeType(DateRange dateRange) {
    if (dateRange.isSingleBounded()) {
      return dateRange.getLowerBound().isUnbounded()
          ? DATE_RANGE_TYPE_SINGLE_DATE_OPEN
          : DATE_RANGE_TYPE_SINGLE_DATE;
    } else {
      DateRangeBound upperBound =
          dateRange
              .getUpperBound()
              .orElseThrow(
                  () ->
                      new IllegalStateException("Upper bound should be set if !isSingleBounded()"));
      if (dateRange.getLowerBound().isUnbounded()) {
        return upperBound.isUnbounded()
            ? DATE_RANGE_TYPE_BOTH_OPEN_RANGE
            : DATE_RANGE_TYPE_OPEN_RANGE_LOW;
      } else {
        return upperBound.isUnbounded()
            ? DATE_RANGE_TYPE_OPEN_RANGE_HIGH
            : DATE_RANGE_TYPE_CLOSED_RANGE;
      }
    }
  }

  private static void put(ByteBuffer buffer, DateRangeBound bound) {
    buffer.putLong(bound.getTimestamp().toInstant().toEpochMilli());
    buffer.put(bound.getPrecision().getEncoding());
  }

  @Nullable
  @Override
  public DateRange decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    }
    byte type = bytes.get();
    switch (type) {
      case DATE_RANGE_TYPE_SINGLE_DATE:
        return new DateRange(decodeLowerBound(bytes));
      case DATE_RANGE_TYPE_CLOSED_RANGE:
        return new DateRange(decodeLowerBound(bytes), decodeUpperBound(bytes));
      case DATE_RANGE_TYPE_OPEN_RANGE_HIGH:
        return new DateRange(decodeLowerBound(bytes), DateRangeBound.UNBOUNDED);
      case DATE_RANGE_TYPE_OPEN_RANGE_LOW:
        return new DateRange(DateRangeBound.UNBOUNDED, decodeUpperBound(bytes));
      case DATE_RANGE_TYPE_BOTH_OPEN_RANGE:
        return new DateRange(DateRangeBound.UNBOUNDED, DateRangeBound.UNBOUNDED);
      case DATE_RANGE_TYPE_SINGLE_DATE_OPEN:
        return new DateRange(DateRangeBound.UNBOUNDED);
      default:
        throw new IllegalArgumentException("Unknown date range type: " + type);
    }
  }

  private static DateRangeBound decodeLowerBound(ByteBuffer bytes) {
    long epochMilli = bytes.getLong();
    ZonedDateTime timestamp =
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
    DateRangePrecision precision = DateRangePrecision.fromEncoding(bytes.get());
    return DateRangeBound.lowerBound(timestamp, precision);
  }

  private static DateRangeBound decodeUpperBound(ByteBuffer bytes) {
    long epochMilli = bytes.getLong();
    ZonedDateTime timestamp =
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
    DateRangePrecision precision = DateRangePrecision.fromEncoding(bytes.get());
    return DateRangeBound.upperBound(timestamp, precision);
  }

  @NonNull
  @Override
  public String format(@Nullable DateRange dateRange) {
    return (dateRange == null) ? "NULL" : Strings.quote(dateRange.toString());
  }

  @Nullable
  @Override
  public DateRange parse(@Nullable String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }
    try {
      return DateRange.parse(Strings.unquote(value));
    } catch (ParseException e) {
      throw new IllegalArgumentException(String.format("Invalid date range literal: %s", value), e);
    }
  }
}
