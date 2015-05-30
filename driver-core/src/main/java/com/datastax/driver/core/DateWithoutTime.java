/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Represents a Cassandra {@code DATE} data type.
 * The {@code DATE} data type is available since Cassandra 2.2 via native protocol V4.
 *
 * @since 2.2
 */
public abstract class DateWithoutTime {

    private static final String DEFAULT_PATTERN = "yyyy-MM-dd";

    private static final long minSupportedDateMillis = TimeUnit.DAYS.toMillis(Integer.MIN_VALUE);
    private static final long maxSupportedDateMillis = TimeUnit.DAYS.toMillis(Integer.MAX_VALUE);

    private final int days;

    public DateWithoutTime(int days) {
        this.days = days;
    }

    public int getDays() {
        return days;
    }

    public long toMillis() {
        return TimeUnit.DAYS.toMillis(this.days + Integer.MIN_VALUE);
    }

    /**
     * Convert a {@code TIMESTAMP} value to a {@code DATE} value.
     * Both values are in UTC and are since epoch (1970-01-01).
     *
     * @param millis input value for {@code TIMESTAMP} type
     * @return value for {@code DATE} type
     *
     * @throws IllegalArgumentException if input value is out of allowed range
     */
    public static DateWithoutTime fromMillis(long millis) throws IllegalArgumentException {
        if (millis < minSupportedDateMillis || millis > maxSupportedDateMillis)
            throw new IllegalArgumentException("Input value out of range");

        int result = (int)TimeUnit.MILLISECONDS.toDays(millis);
        result -= Integer.MIN_VALUE;
        return new DefaultDateWithoutTime(result);
    }

    /**
     * Convert a Cassandra {@code DATE} raw representation, which is an {@code int}, to
     * a {@code DateWithoutTime} instance.
     *
     * @param days Cassandra {@code DATE} raw value
     * @return {@code DateWithoutTime} instance.
     */
    public static DateWithoutTime fromSimpleDate(int days) {
        return new DefaultDateWithoutTime(days);
    }

    // not public since it has no timezone support
    static long timeToMillis(long time) {
        return TimeUnit.NANOSECONDS.toMillis(time);
    }

    /**
     * Convert this date-without-time + {@code TIME} with a timezone to a Java
     * {@link Calendar} object.
     *
     * @param time {@code TIME} value
     * @param target the Calendar object initialized with the correct time zone
     * @return modified Calendar object
     */
    public Calendar toCalendar(long time, Calendar target) {
        long millis = toMillis();
        target.setTimeInMillis(millis);
        time = timeToMillis(time);
        target.set(Calendar.MILLISECOND, (int) (time % 1000L));
        time /= 1000L;
        target.set(Calendar.SECOND, (int) (time % 60));
        time /= 60L;
        target.set(Calendar.MINUTE, (int) (time % 60));
        time /= 60L;
        target.set(Calendar.HOUR_OF_DAY, (int) (time % 24));
        return target;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DateWithoutTime that = (DateWithoutTime) o;

        return days == that.days;
    }

    public int hashCode()
    {
        return days;
    }

    public String toString() {
        SimpleDateFormat parser = new SimpleDateFormat();
        parser.setLenient(false);
        parser.setTimeZone(TimeZone.getTimeZone("UTC"));
        parser.applyPattern(DEFAULT_PATTERN);
        return parser.format(toMillis());
    }

    /**
     * Internal default implementation.
     */
    static final class DefaultDateWithoutTime extends DateWithoutTime {
        public DefaultDateWithoutTime(int days) {
            super(days);
        }
    }

}
