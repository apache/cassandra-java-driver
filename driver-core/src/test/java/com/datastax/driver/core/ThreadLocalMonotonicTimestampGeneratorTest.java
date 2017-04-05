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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class ThreadLocalMonotonicTimestampGeneratorTest {

    @Test(groups = "unit")
    public void should_generate_incrementing_timestamps_for_each_thread() throws InterruptedException {
        // Create a generator with a fixed millisecond value
        final long fixedTime = 1;
        final ThreadLocalMonotonicTimestampGenerator generator = new ThreadLocalMonotonicTimestampGenerator();
        generator.clock = new MockClocks.FixedTimeClock(fixedTime);

        // Generate 1000 timestamps on each thread
        int testThreadsCount = 2;
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(testThreadsCount));

        List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(testThreadsCount);
        for (int i = 0; i < testThreadsCount; i++) {
            futures.add(executor.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            // Ensure that each thread gets the 1000 microseconds for the mocked millisecond value,
                            // in order
                            for (int i = 0; i < 1000; i++)
                                assertEquals(generator.next(), fixedTime + i);
                        }
                    }));
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);

        try {
            Futures.allAsList(futures).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof AssertionError)
                throw (AssertionError) cause;
            else
                fail("Error in a test thread", cause);
        }
    }

    @Test(groups = "unit")
    public void should_generate_incrementing_timestamps_on_clock_resync() {
        ThreadLocalMonotonicTimestampGenerator generator = new ThreadLocalMonotonicTimestampGenerator(0, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
        generator.clock = new MockClocks.BackInTimeClock();

        MemoryAppender appender = new MemoryAppender();
        Logger logger = Logger.getLogger(TimestampGenerator.class);
        Level originalLevel = logger.getLevel();
        logger.setLevel(Level.WARN);
        logger.addAppender(appender);
        String logFormat = "Clock skew detected: current tick (%d) was %d microseconds " +
                "behind the last generated timestamp (%d), returned timestamps will be artificially incremented " +
                "to guarantee monotonicity.";

        try {
            long start = generator.next();
            long previous = start;
            long next = 0;
            for (int i = 0; i < 1001; i++) {
                next = generator.next();
                assertEquals(next, previous + 1);
                previous = next;
            }

            // Ensure log statement generated indicating clock skew, but only once.
            assertEquals(next, start + 1001);
            assertThat(appender.getNext())
                    .containsOnlyOnce("Clock skew detected:")
                    .containsOnlyOnce(String.format(logFormat, start - 1, 1, start));

            // Wait for 1.1 seconds to see if we get an additional clock skew message.  We wait slightly longer
            // than 1 second to deal with system clock precision on platforms like windows.
            Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);

            next = generator.next();
            assertThat(next).isEqualTo(previous + 1);
            // Clock has gone backwards 1002 us since we've had that many iterations.
            // The difference should be 2003 (clock backwards 1002 + 1001 prior compute next calls).
            // Current timestamp should match the previous one.
            assertThat(appender.getNext())
                    .containsOnlyOnce("Clock skew detected:")
                    .containsOnlyOnce(String.format(logFormat, start - 1002, 2003, previous));
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(originalLevel);
        }
    }
}
