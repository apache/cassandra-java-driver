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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TimestampsTest
{
    @Test(groups = "unit")
    public void conformanceTest() {
        // -2147467577 : 2014-01-01
        // -2147483648 : 1970-01-01
        long millis = Timestamps.simpleDateToMillis(-2147467577);
        int days = Timestamps.millisToSimpleDate(millis);
        assertEquals(days, -2147467577);
        // 1 hour later - must be same day
        days = Timestamps.millisToSimpleDate(millis + 360000L);
        assertEquals(days, -2147467577);
        // 2 hours later - must be same day
        days = Timestamps.millisToSimpleDate(millis + 720000L);
        assertEquals(days, -2147467577);

        millis = Timestamps.simpleDateToMillis(-2147483648);
        days = Timestamps.millisToSimpleDate(millis);
        assertEquals(days, -2147483648);
        // 1 hour later - must be same day
        days = Timestamps.millisToSimpleDate(millis + 360000L);
        assertEquals(days, -2147483648);
        // 2 hours later - must be same day
        days = Timestamps.millisToSimpleDate(millis + 720000L);
        assertEquals(days, -2147483648);
    }
}
