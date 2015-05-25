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

import java.util.Calendar;
import java.util.TimeZone;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class DateWithoutTimeTest
{
    @Test(groups = "unit")
    public void conformanceTest() {
        // -2147467577 --> 16071 --> 2014-01-01
        // -2147483648 -> 0      --> 1970-01-01
        DateWithoutTime dateWithoutTime = new DateWithoutTime(16071);
        long millis = dateWithoutTime.toMillis();
        int days = DateWithoutTime.fromMillis(millis).getDaysSinceEpoch();
        assertEquals(days, 16071);
        // 1 hour later - must be same day
        days = DateWithoutTime.fromMillis(millis + 360000L).getDaysSinceEpoch();
        assertEquals(days, 16071);
        // 2 hours later - must be same day
        days = DateWithoutTime.fromMillis(millis + 720000L).getDaysSinceEpoch();
        assertEquals(days, 16071);

        dateWithoutTime = new DateWithoutTime(0);
        millis = dateWithoutTime.toMillis();
        days = DateWithoutTime.fromMillis(millis).getDaysSinceEpoch();
        assertEquals(days, 0);
        // 1 hour later - must be same day
        days = DateWithoutTime.fromMillis(millis + 360000L).getDaysSinceEpoch();
        assertEquals(days, 0);
        // 2 hours later - must be same day
        days = DateWithoutTime.fromMillis(millis + 720000L).getDaysSinceEpoch();
        assertEquals(days, 0);
    }

    @Test(groups = "unit")
    public void fromYearMonthDayTest() {
        DateWithoutTime dwt = DateWithoutTime.fromYearMonthDay(2014, 0, 1);
        assertEquals(dwt.getDaysSinceEpoch(), 16071);
        assertEquals(dwt.getDay(), 1);
        assertEquals(dwt.getMonth(), 0);
        assertEquals(dwt.getYear(), 2014);
    }

    @Test(groups = "unit")
    public void rangeTest() {
        // minimum value
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTimeInMillis(0L);
        cal.add(Calendar.DAY_OF_MONTH, Integer.MIN_VALUE);
        DateWithoutTime dwt = DateWithoutTime.fromMillis(cal.getTimeInMillis());
        assertEquals(dwt.toMillis(), cal.getTimeInMillis());

        // maximum value
        cal.setTimeInMillis(0L);
        cal.add(Calendar.DAY_OF_MONTH, Integer.MAX_VALUE);
        dwt = DateWithoutTime.fromMillis(cal.getTimeInMillis());
        assertEquals(dwt.toMillis(), cal.getTimeInMillis());

        // some years/months iterations (10000 yrs seems enough)
        for (int i=0; i < 365*10000; i += 30) {
            cal.setTimeInMillis(0L);
            cal.add(Calendar.DAY_OF_MONTH, i);
            dwt = DateWithoutTime.fromMillis(cal.getTimeInMillis());
            assertEquals(dwt.toMillis(), cal.getTimeInMillis());
        }

        // some years/months iterations (10000 yrs seems enough)
        for (int i=0; i > -365*10000; i -= 30) {
            cal.setTimeInMillis(0L);
            cal.add(Calendar.DAY_OF_MONTH, i);
            dwt = DateWithoutTime.fromMillis(cal.getTimeInMillis());
            assertEquals(dwt.toMillis(), cal.getTimeInMillis());
        }
    }
}
