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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation for generators based on {@link System#currentTimeMillis()} and a counter to generate
 * the sub-millisecond part.
 */
abstract class AbstractMonotonicTimestampGenerator implements TimestampGenerator {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMonotonicTimestampGenerator.class);

    volatile Clock clock = new SystemClock();

    protected long computeNext(long last) {
        long millis = last / 1000;
        long counter = last % 1000;

        long now = clock.currentTime();

        // System.currentTimeMillis can go backwards on an NTP resync, hence the ">" below
        if (millis >= now) {
            if (counter == 999)
                logger.warn("Sub-millisecond counter overflowed, some query timestamps will not be distinct");
            else
                counter += 1;
        } else {
            millis = now;
            counter = 0;
        }

        return millis * 1000 + counter;
    }
}
