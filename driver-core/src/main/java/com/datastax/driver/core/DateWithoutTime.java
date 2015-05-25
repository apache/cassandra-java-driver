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
 * Note: the internal representation of this class uses a {@code long}
 *
 * @since 2.2
 */
public final class DateWithoutTime {

    private static final String DEFAULT_PATTERN = "yyyy-MM-dd";
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private final int days;

    /**
     * Convert a days-since-epoch value (epoch==0) to a {@code DateWithoutTime} instance.
     *
     * @param daysSinceEpoch days since epoch (1970-01-01)
     */
    public DateWithoutTime(int daysSinceEpoch) {
        this.days = daysSinceEpoch;
    }

    /**
     * Get the days-since-epoch value.
     */
    public int getDaysSinceEpoch() {
        return days;
    }

    /**
     * Get the year that this date represents.
     */
    public int getYear() {
        return toCalendar().get(Calendar.YEAR);
    }

    /**
     * Get the month that this date represents.
     * @return month value is 0-based. e.g., 0 for January.
     */
    public int getMonth() {
        return toCalendar().get(Calendar.MONTH);
    }

    /**
     * Get the day-of-month that this date represents.
     */
    public int getDay() {
        return toCalendar().get(Calendar.DAY_OF_MONTH);
    }

    /**
     * Convert this date-without-time value to milliseconds since epoch.
     */
    public long toMillis() {
        return toCalendar().getTimeInMillis();
    }

    /**
     * Convert this date-without-time value to a {@link Calendar} instance.
     */
    public Calendar toCalendar() {
        Calendar cal = Calendar.getInstance(UTC);
        cal.clear();
        cal.add(Calendar.DAY_OF_YEAR, days);
        return cal;
    }

    /**
     * Convert a milliseconds-since-epoch (1970-01-01) value to a {@code DateWithoutTime}.
     *
     * @param millis input value
     * @return corresponding DateWithoutTime for input value
     */
    public static DateWithoutTime fromMillis(long millis) throws IllegalArgumentException {
        int result = (int)TimeUnit.MILLISECONDS.toDays(millis);
        return new DateWithoutTime(result);
    }

    /**
     * Convert year, month, day to a {@code DateWithoutTime}.
     *
     * @param year the value used to set the <code>YEAR</code> calendar field.
     * @param month the value used to set the <code>MONTH</code> calendar field.
     *              Month value is 0-based. e.g., 0 for January.
     * @param day the value used to set the <code>DAY_OF_MONTH</code> calendar field.
     * @return converted {@code DateWithoutTime}
     */
    public static DateWithoutTime fromYearMonthDay(int year, int month, int day) {
        Calendar cal = Calendar.getInstance(UTC);
        cal.clear();
        cal.set(year, month, day, 0, 0, 0);
        return fromMillis(cal.getTimeInMillis());
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
        SimpleDateFormat parser = new SimpleDateFormat(DEFAULT_PATTERN);
        parser.setLenient(false);
        parser.setTimeZone(UTC);
        return parser.format(toMillis());
    }

}
