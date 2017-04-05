/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import org.testng.annotations.Test;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.LocalDate.*;

public class LocalDateTest {

    @Test(groups = "unit")
    public void should_build_from_days_since_epoch() {
        assertThat(fromDaysSinceEpoch(0))
                .hasMillisSinceEpoch(0)
                .hasDaysSinceEpoch(0)
                .hasYearMonthDay(1970, 1, 1)
                .hasToString("1970-01-01");

        assertThat(fromDaysSinceEpoch(10))
                .hasMillisSinceEpoch(TimeUnit.DAYS.toMillis(10))
                .hasDaysSinceEpoch(10)
                .hasYearMonthDay(1970, 1, 11)
                .hasToString("1970-01-11");

        assertThat(fromDaysSinceEpoch(-10))
                .hasMillisSinceEpoch(TimeUnit.DAYS.toMillis(-10))
                .hasDaysSinceEpoch(-10)
                .hasYearMonthDay(1969, 12, 22)
                .hasToString("1969-12-22");

        assertThat(fromDaysSinceEpoch(Integer.MAX_VALUE))
                .hasMillisSinceEpoch(TimeUnit.DAYS.toMillis(Integer.MAX_VALUE))
                .hasDaysSinceEpoch(Integer.MAX_VALUE)
                .hasYearMonthDay(5881580, 7, 11)
                .hasToString("5881580-07-11");

        assertThat(fromDaysSinceEpoch(Integer.MIN_VALUE))
                .hasMillisSinceEpoch(TimeUnit.DAYS.toMillis(Integer.MIN_VALUE))
                .hasDaysSinceEpoch(Integer.MIN_VALUE)
                .hasYearMonthDay(-5877641, 6, 23)
                .hasToString("-5877641-06-23");
    }

    @Test(groups = "unit")
    public void should_build_from_millis_since_epoch() {
        assertThat(fromMillisSinceEpoch(0))
                .hasMillisSinceEpoch(0)
                .hasDaysSinceEpoch(0)
                .hasYearMonthDay(1970, 1, 1)
                .hasToString("1970-01-01");

        // Rounding
        assertThat(fromMillisSinceEpoch(3600))
                .hasMillisSinceEpoch(0)
                .hasDaysSinceEpoch(0)
                .hasYearMonthDay(1970, 1, 1)
                .hasToString("1970-01-01");
        assertThat(fromMillisSinceEpoch(-3600))
                .hasMillisSinceEpoch(0)
                .hasDaysSinceEpoch(0)
                .hasYearMonthDay(1970, 1, 1)
                .hasToString("1970-01-01");

        // Bound checks
        try {
            fromMillisSinceEpoch(TimeUnit.DAYS.toMillis((long) Integer.MIN_VALUE - 1));
            Assertions.fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
        try {
            fromMillisSinceEpoch(TimeUnit.DAYS.toMillis((long) Integer.MAX_VALUE + 1));
            Assertions.fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
    }

    @Test(groups = "unit")
    public void should_build_from_year_month_day() {
        assertThat(fromYearMonthDay(1970, 1, 1))
                .hasMillisSinceEpoch(0)
                .hasDaysSinceEpoch(0)
                .hasYearMonthDay(1970, 1, 1)
                .hasToString("1970-01-01");

        // Handling of 0 / negative years
        assertThat(fromYearMonthDay(1, 1, 1))
                .hasDaysSinceEpoch(-719162)
                .hasYearMonthDay(1, 1, 1)
                .hasToString("1-01-01");
        assertThat(fromYearMonthDay(0, 1, 1))
                .hasDaysSinceEpoch(-719162 - 366)
                .hasYearMonthDay(0, 1, 1)
                .hasToString("0-01-01");
        assertThat(fromYearMonthDay(-1, 1, 1))
                .hasDaysSinceEpoch(-719162 - 366 - 365)
                .hasYearMonthDay(-1, 1, 1)
                .hasToString("-1-01-01");

        // Month/day out of bounds
        try {
            fromYearMonthDay(1970, 0, 1);
            Assertions.fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
        try {
            fromYearMonthDay(1970, 13, 1);
            Assertions.fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
        try {
            fromYearMonthDay(1970, 1, 0);
            Assertions.fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
        try {
            fromYearMonthDay(1970, 1, 32);
            Assertions.fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }

        // Resulting date out of bounds
        try {
            fromYearMonthDay(6000000, 1, 1);
            Assertions.fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
        try {
            fromYearMonthDay(-6000000, 1, 1);
            Assertions.fail("Expected an IllegalArgumentException");
        } catch (IllegalArgumentException e) { /*expected*/ }
    }

    @Test(groups = "unit")
    public void should_add_and_subtract_years() {
        assertThat(fromYearMonthDay(1970, 1, 1).add(Calendar.YEAR, 1))
                .hasYearMonthDay(1971, 1, 1);
        assertThat(fromYearMonthDay(1970, 1, 1).add(Calendar.YEAR, -1))
                .hasYearMonthDay(1969, 1, 1);
        assertThat(fromYearMonthDay(1970, 1, 1).add(Calendar.YEAR, -1970))
                .hasYearMonthDay(0, 1, 1);
        assertThat(fromYearMonthDay(1970, 1, 1).add(Calendar.YEAR, -1971))
                .hasYearMonthDay(-1, 1, 1);
        assertThat(fromYearMonthDay(0, 5, 12).add(Calendar.YEAR, 1))
                .hasYearMonthDay(1, 5, 12);
        assertThat(fromYearMonthDay(-1, 5, 12).add(Calendar.YEAR, 1))
                .hasYearMonthDay(0, 5, 12);
        assertThat(fromYearMonthDay(-1, 5, 12).add(Calendar.YEAR, 2))
                .hasYearMonthDay(1, 5, 12);
    }

    @Test(groups = "unit")
    public void should_add_and_subtract_months() {
        assertThat(fromYearMonthDay(1970, 1, 1).add(Calendar.MONTH, 2))
                .hasYearMonthDay(1970, 3, 1);
        assertThat(fromYearMonthDay(1970, 1, 1).add(Calendar.MONTH, 24))
                .hasYearMonthDay(1972, 1, 1);
        assertThat(fromYearMonthDay(1970, 1, 1).add(Calendar.MONTH, -5))
                .hasYearMonthDay(1969, 8, 1);
        assertThat(fromYearMonthDay(1, 1, 1).add(Calendar.MONTH, -1))
                .hasYearMonthDay(0, 12, 1);
        assertThat(fromYearMonthDay(0, 1, 1).add(Calendar.MONTH, -1))
                .hasYearMonthDay(-1, 12, 1);
        assertThat(fromYearMonthDay(-1, 12, 1).add(Calendar.MONTH, 1))
                .hasYearMonthDay(0, 1, 1);
        assertThat(fromYearMonthDay(0, 12, 1).add(Calendar.MONTH, 1))
                .hasYearMonthDay(1, 1, 1);
    }

    @Test(groups = "unit")
    public void should_add_and_subtract_days() {
        assertThat(fromYearMonthDay(1970, 1, 1).add(Calendar.DAY_OF_MONTH, 12))
                .hasYearMonthDay(1970, 1, 13);
        assertThat(fromYearMonthDay(1970, 3, 28).add(Calendar.DAY_OF_MONTH, -40))
                .hasYearMonthDay(1970, 2, 16);
        assertThat(fromYearMonthDay(1, 1, 1).add(Calendar.DAY_OF_MONTH, -2))
                .hasYearMonthDay(0, 12, 30);
        assertThat(fromYearMonthDay(0, 1, 1).add(Calendar.DAY_OF_MONTH, -2))
                .hasYearMonthDay(-1, 12, 30);
        assertThat(fromYearMonthDay(-1, 12, 31).add(Calendar.DAY_OF_MONTH, 4))
                .hasYearMonthDay(0, 1, 4);
        assertThat(fromYearMonthDay(0, 12, 25).add(Calendar.DAY_OF_MONTH, 14))
                .hasYearMonthDay(1, 1, 8);
    }
}
