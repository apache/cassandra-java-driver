package com.datastax.driver.core;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
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
                                            assertEquals(generator.next(), fixedTime * 1000 + i);
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
                throw (AssertionError)cause;
            else
                fail("Error in a test thread", cause);
        }
    }

    @Test(groups = "unit")
    public void should_generate_incrementing_timestamps_on_clock_resync() {
        ThreadLocalMonotonicTimestampGenerator generator = new ThreadLocalMonotonicTimestampGenerator();
        generator.clock = new MockClocks.BackInTimeClock();

        long beforeClockResync = generator.next();
        long afterClockResync = generator.next();

        System.out.println(beforeClockResync);
        System.out.println(afterClockResync);

        assertTrue(beforeClockResync < afterClockResync, "The generated timestamps are not increasing on block resync");
    }
}
