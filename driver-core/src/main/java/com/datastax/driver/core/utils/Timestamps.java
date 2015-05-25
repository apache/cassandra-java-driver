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
package com.datastax.driver.core.utils;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

/**
 * Helper functions to deal with {@code TIMESTAMP}, {@code DATE} and {@code TIME} types.
 */
public final class Timestamps {
    private Timestamps() {}

    private static final long minSupportedDateMillis = TimeUnit.DAYS.toMillis(Integer.MIN_VALUE);
    private static final long maxSupportedDateMillis = TimeUnit.DAYS.toMillis(Integer.MAX_VALUE);

    /**
     * Convert a {@code DATE} value to a {@code TIMESTAMP} value.
     * Both values are in UTC and are since epoch (1970-01-01).
     *
     * @param date input value for {@code DATE} type
     * @return value for {@code TIMESTAMP} type
     */
    public static long simpleDateToMillis(int date) {
        long days = date + Integer.MIN_VALUE;
        return TimeUnit.DAYS.toMillis(days);
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
    public static int millisToSimpleDate(long millis) throws IllegalArgumentException {
        if (millis < minSupportedDateMillis || millis > maxSupportedDateMillis)
            throw new IllegalArgumentException("Input value out of range");

        int result = (int)TimeUnit.MILLISECONDS.toDays(millis);
        result -= Integer.MIN_VALUE;
        return result;
    }

    // not public since it has no timezone support
    static long timeToMillis(long time) {
        return TimeUnit.NANOSECONDS.toMillis(time);
    }

    /**
     * Convert a {@code DATE} + {@code TIME} with a timezone to a Java {@link Calendar} object.
     *
     * @param date {@code DATE} value
     * @param time {@code TIME} value
     * @param target the Calendar object initialized with the correct time zone
     * @return modified Calendar object
     */
    public static Calendar toCalendar(int date, long time, Calendar target) {
        long millis = simpleDateToMillis(date);
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

    // NOTE: Be careful with additional methods that work on Joda types or Calendar since C* TIME type
    // does not handle time zone - i.e. time changes from/to summer/winter time are not handled.
    //
    // For example:
    // 2015-03-29 02:30 is illegal (it just does not exist, time is advanced from 02:00 to 03:00)
    // 2015-10-25 02:30 is ambiguous without CET or CEST (one hour difference, time is set back from 03:00 CEST to 02:00 CET)
}
