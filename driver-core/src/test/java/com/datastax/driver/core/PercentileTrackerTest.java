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

import com.datastax.driver.core.exceptions.*;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public abstract class PercentileTrackerTest<B extends PercentileTracker.Builder<B, T>, T extends PercentileTracker> {

    Host defaultHost = mock(Host.class);

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    Exception defaultException = mock(Exception.class);

    Statement defaultStatement = mock(Statement.class);

    long defaultMaxLatency = 1000;

    public abstract B builder();

    @Test(groups = "unit")
    public void should_ignore_certain_exceptions() throws Exception {
        // given - a percentile tracker.
        Cluster cluster0 = mock(Cluster.class);
        T tracker = builder()
                .withInterval(50, TimeUnit.MILLISECONDS)
                .withMinRecordedValues(100).build();
        tracker.onRegister(cluster0);

        // when - recording measurements with the following exceptions.
        List<Exception> exceptionsToIgnore = Lists.<Exception>newArrayList(
                new UnavailableException(ConsistencyLevel.ANY, 0, 0),
                new OverloadedException(null, "Overloaded"),
                new BootstrappingException(null, "Bootstrapping"),
                new UnpreparedException(null, "Unprepared"),
                new InvalidQueryException("Validation", new Exception()));

        long startTime = System.currentTimeMillis();
        for (Exception exception : exceptionsToIgnore) {
            tracker.update(defaultHost, defaultStatement, exception, TimeUnit.NANOSECONDS.convert(999, TimeUnit.MILLISECONDS));
        }
        for (int i = 0; i < 100; i++) {
            tracker.update(defaultHost, defaultStatement, defaultException, TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS));
        }
        long waitTime = 50 - (System.currentTimeMillis() - startTime);
        Uninterruptibles.sleepUninterruptibly(waitTime + 100, TimeUnit.MILLISECONDS);

        // then - the resulting tracker's percentiles should all be 1, indicating those exceptions were ignored.
        for (int i = 1; i <= 99; i++) {
            long latencyAtPct = tracker.getLatencyAtPercentile(defaultHost, defaultStatement, defaultException, i);
            assertThat(latencyAtPct).isEqualTo(1);
        }
    }

    @Test(groups = "unit")
    public void should_not_record_anything_if_not_enough_measurements() throws Exception {
        // given - a percentile tracker with 100 min recorded values.
        Cluster cluster0 = mock(Cluster.class);
        T tracker = builder()
                .withInterval(50, TimeUnit.MILLISECONDS)
                .withMinRecordedValues(100).build();
        tracker.onRegister(cluster0);

        // when - recording less measurements then required.
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 99; i++) {
            tracker.update(defaultHost, defaultStatement, defaultException, TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS));
        }
        long waitTime = 50 - (System.currentTimeMillis() - startTime);
        Uninterruptibles.sleepUninterruptibly(waitTime + 100, TimeUnit.MILLISECONDS);

        // then - the resulting tracker's percentiles should all be -1, indicating there were not enough values to consider.
        for (int i = 1; i <= 99; i++) {
            long latencyAtPct = tracker.getLatencyAtPercentile(defaultHost, defaultStatement, defaultException, i);
            assertThat(latencyAtPct).isEqualTo(-1);
        }
    }

    @Test(groups = "short")
    public void should_return_negative_value_when_interval_hasnt_elapsed() throws Exception {
        // given - a percentile tracker with a long interval.
        Cluster cluster0 = mock(Cluster.class);
        T tracker = builder()
                .withInterval(50, TimeUnit.MINUTES)
                .withMinRecordedValues(100).build();
        tracker.onRegister(cluster0);

        // when - recording enough measurements.
        for (int i = 0; i < 99; i++) {
            tracker.update(defaultHost, defaultStatement, defaultException, TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS));
        }

        // then - the resulting tracker's percentiles should all be -1, since not enough time was given to elapse interval.
        for (int i = 1; i <= 99; i++) {
            long latencyAtPct = tracker.getLatencyAtPercentile(defaultHost, defaultStatement, defaultException, i);
            assertThat(latencyAtPct).isEqualTo(-1);
        }
    }

    @Test(groups = "unit")
    public void should_not_record_value_and_log_when_measurement_higher_than_max_trackable_value() throws Exception {
        // given - a percentile tracker with a long interval.
        Cluster cluster0 = mock(Cluster.class);
        T tracker = builder()
                .withInterval(50, TimeUnit.MILLISECONDS)
                .withMinRecordedValues(100).build();
        tracker.onRegister(cluster0);

        Logger percentileLogger = Logger.getLogger(PercentileTracker.class);
        Level originalLevel = percentileLogger.getLevel();
        percentileLogger.setLevel(Level.WARN);
        MemoryAppender appender = new MemoryAppender();
        percentileLogger.addAppender(appender);

        try {
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                tracker.update(defaultHost, defaultStatement, defaultException, TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS));
            }

            // HdrHistogram adjusts its max based on bucket size, with these values it allows a max value of 2047 (2^11-1).
            long largeLatency = 2048;
            // when - recording a value larger than max trackable value.
            tracker.update(defaultHost, defaultStatement, defaultException, TimeUnit.NANOSECONDS.convert(largeLatency, TimeUnit.MILLISECONDS));

            assertThat(appender.get())
                    .contains("Got request with latency of " + largeLatency
                            + " ms, which exceeds the configured maximum trackable value " + defaultMaxLatency);

            long waitTime = 50 - (System.currentTimeMillis() - startTime);
            Uninterruptibles.sleepUninterruptibly(waitTime + 100, TimeUnit.MILLISECONDS);

            // then - the resulting tracker's percentiles should all be 1, indicating the large value was ignored.
            for (int i = 1; i <= 99; i++) {
                long latencyAtPct = tracker.getLatencyAtPercentile(defaultHost, defaultStatement, defaultException, i);
                assertThat(latencyAtPct).isEqualTo(1);
            }
        } finally {
            percentileLogger.setLevel(originalLevel);
            percentileLogger.removeAppender(appender);
        }
    }
}
