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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.*;

/**
 * A small abstraction around system clock that aims to provide microsecond precision with the best accuracy possible.
 */
interface Clock {

    /**
     * Returns the current time in microseconds.
     *
     * @return the difference, measured in microseconds, between the current time and and the Epoch
     * (that is, midnight, January 1, 1970 UTC).
     */
    long currentTimeMicros();
}

/**
 * Factory that returns the best Clock implementation depending on what native libraries are available in the system.
 * If LibC is available through JNR, and if the system property {@code com.datastax.driver.USE_NATIVE_CLOCK} is set to {@code true}
 * (which is the default value), then {@link NativeClock} is returned, otherwise {@link SystemClock} is returned.
 */
class ClockFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClockFactory.class);

    private static final String USE_NATIVE_CLOCK_SYSTEM_PROPERTY = "com.datastax.driver.USE_NATIVE_CLOCK";

    static Clock newInstance() {
        if (SystemProperties.getBoolean(USE_NATIVE_CLOCK_SYSTEM_PROPERTY, true) && Native.isGettimeofdayAvailable()) {
            LOGGER.info("Using native clock to generate timestamps.");
            return new NativeClock();
        } else {
            LOGGER.info("Using java.lang.System clock to generate timestamps.");
            return new SystemClock();
        }
    }

}

/**
 * Default implementation of a clock that delegates its calls to the system clock.
 *
 * @see System#currentTimeMillis()
 */
class SystemClock implements Clock {

    @Override
    public long currentTimeMicros() {
        return System.currentTimeMillis() * 1000;
    }

}

/**
 * Provides the current time with microseconds precision with some reasonable accuracy through
 * the use of {@link Native#currentTimeMicros()}.
 * <p/>
 * Because calling JNR methods is slightly expensive,
 * we only call it once per second and add the number of nanoseconds since the last call
 * to get the current time, which is good enough an accuracy for our purpose (see CASSANDRA-6106).
 * <p/>
 * This reduces the cost of the call to {@link NativeClock#currentTimeMicros()} to levels comparable
 * to those of a call to {@link System#nanoTime()}.
 */
class NativeClock implements Clock {

    private static final long ONE_SECOND_NS = NANOSECONDS.convert(1, SECONDS);
    private static final long ONE_MILLISECOND_NS = NANOSECONDS.convert(1, MILLISECONDS);

    /**
     * Records a time in micros along with the System.nanoTime() value at the time the
     * time is fetched.
     */
    private static class FetchedTime {

        private final long timeInMicros;
        private final long nanoTimeAtCheck;

        private FetchedTime(long timeInMicros, long nanoTimeAtCheck) {
            this.timeInMicros = timeInMicros;
            this.nanoTimeAtCheck = nanoTimeAtCheck;
        }
    }

    private final AtomicReference<FetchedTime> lastFetchedTime = new AtomicReference<FetchedTime>(fetchTimeMicros());

    @Override
    public long currentTimeMicros() {
        FetchedTime spec = lastFetchedTime.get();
        long curNano = System.nanoTime();
        if (curNano > spec.nanoTimeAtCheck + ONE_SECOND_NS) {
            lastFetchedTime.compareAndSet(spec, spec = fetchTimeMicros());
        }
        return spec.timeInMicros + ((curNano - spec.nanoTimeAtCheck) / 1000);
    }

    private static FetchedTime fetchTimeMicros() {
        // To compensate for the fact that the Native.currentTimeMicros call could take
        // some time, instead of picking the nano time before the call or after the
        // call, we take the average of both.
        long start = System.nanoTime();
        long micros = Native.currentTimeMicros();
        long end = System.nanoTime();
        // If it turns out the call took us more than 1 millisecond (can happen while
        // the JVM warms up, unlikely otherwise, but no reasons to take risks), fall back
        // to System.currentTimeMillis() temporarily
        if ((end - start) > ONE_MILLISECOND_NS)
            return new FetchedTime(System.currentTimeMillis() * 1000, System.nanoTime());
        return new FetchedTime(micros, (end + start) / 2);
    }

}
