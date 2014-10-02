package com.datastax.driver.core;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.Test;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class CountingTimestampGeneratorTest {
    private static final int THREAD_COUNT = 2;

    @Test(groups = "unit")
    public void should_generate_incrementing_timestamps() {
        final CountingTimestampGenerator generator = new CountingTimestampGenerator();

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(THREAD_COUNT));
        final CountDownLatch latch = new CountDownLatch(1);

        List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            futures.add(executor.submit(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            latch.await();
                                        } catch (InterruptedException e) {
                                            fail("Interrupted while waiting for test start");
                                        }

                                        long previous = (System.currentTimeMillis() - 1) * 1000;

                                        for (int i = 0; i < 10000; i++) {
                                            long timestamp = generator.next();
                                            assertTrue(timestamp > previous,
                                                       String.format("iteration %d, timestamp=%d, previous=%d",
                                                                     i, timestamp, previous));
                                            previous = timestamp;
                                        }

                                        assertTrue((System.currentTimeMillis() + 1) * 1000 > previous);
                                    }
                                }));
        }
        executor.shutdown();

        latch.countDown();
        try {
            Futures.allAsList(futures).get();
        } catch (InterruptedException e) {
            fail("Interrupted while getting future");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof AssertionError)
                throw (AssertionError)cause;
            else
                fail("Error in a test thread", cause);
        }
    }
}
