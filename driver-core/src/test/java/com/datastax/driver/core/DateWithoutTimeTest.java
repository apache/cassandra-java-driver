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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class DateWithoutTimeTest
{
    @Test(groups = "unit")
    public void conformanceTest() {
        // -2147467577 : 2014-01-01
        // -2147483648 : 1970-01-01
        DateWithoutTime dateWithoutTime = DateWithoutTime.fromSimpleDate(-2147467577);
        long millis = dateWithoutTime.toMillis();
        int days = DateWithoutTime.fromMillis(millis).getDays();
        assertEquals(days, -2147467577);
        // 1 hour later - must be same day
        days = DateWithoutTime.fromMillis(millis + 360000L).getDays();
        assertEquals(days, -2147467577);
        // 2 hours later - must be same day
        days = DateWithoutTime.fromMillis(millis + 720000L).getDays();
        assertEquals(days, -2147467577);

        dateWithoutTime = DateWithoutTime.fromSimpleDate(-2147483648);
        millis = dateWithoutTime.toMillis();
        days = DateWithoutTime.fromMillis(millis).getDays();
        assertEquals(days, -2147483648);
        // 1 hour later - must be same day
        days = DateWithoutTime.fromMillis(millis + 360000L).getDays();
        assertEquals(days, -2147483648);
        // 2 hours later - must be same day
        days = DateWithoutTime.fromMillis(millis + 720000L).getDays();
        assertEquals(days, -2147483648);
    }
}
