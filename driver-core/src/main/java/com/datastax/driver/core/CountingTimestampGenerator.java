/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
 * A timestamp generator based on {@code System.currentTimeMillis()}, with an incrementing thread-local counter
 * to generate the sub-millisecond part.
 */
public class CountingTimestampGenerator implements TimestampGenerator {
    private static final Logger logger = LoggerFactory.getLogger(CountingTimestampGenerator.class);

    // We're deliberately avoiding an anonymous subclass with initialValue(), because this can introduce
    // classloader leaks in managed environments like Tomcat
    private final ThreadLocal<Long> last = new ThreadLocal<Long>();

    @Override
    public long next() {
        Long micros = this.last.get();
        if (micros == null)
            micros = 0L;

        long millis = micros / 1000;
        long counter = micros % 1000;

        long now = System.currentTimeMillis();
        if (millis == now) {
            if (counter == 999)
                // This should not happen: even if System.currentTimeMillis() does not always have millisecond
                // precision, it's unlikely that a single thread will generate timestamps at that rate
                logger.warn("Sub-millisecond counter overflowed, some query timestamps will not be distinct");
            else
                counter += 1;
        } else {
            millis = now;
            counter = 0;
        }

        micros = millis * 1000 + counter;

        this.last.set(micros);
        return micros;
    }
}
