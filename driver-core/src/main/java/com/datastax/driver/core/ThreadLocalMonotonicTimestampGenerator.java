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


/**
 * A timestamp generator based on {@code System.currentTimeMillis()}, with an incrementing thread-local counter
 * to generate the sub-millisecond part.
 * <p/>
 * This implementation guarantees incrementing timestamps for a given client thread, provided that no more than
 * 1000 are requested for a given clock tick (the exact granularity of of {@link System#currentTimeMillis()}
 * depends on the operating system).
 * <p/>
 * If that rate is exceeded, a warning is logged and the timestamps don't increment anymore until the next clock
 * tick.
 */
public class ThreadLocalMonotonicTimestampGenerator extends AbstractMonotonicTimestampGenerator {
    // We're deliberately avoiding an anonymous subclass with initialValue(), because this can introduce
    // classloader leaks in managed environments like Tomcat
    private final ThreadLocal<Long> lastRef = new ThreadLocal<Long>();

    @Override
    public long next() {
        Long last = this.lastRef.get();
        if (last == null)
            last = 0L;

        long next = computeNext(last);

        this.lastRef.set(next);
        return next;
    }
}
