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

import com.datastax.driver.core.AbstractReconnectionHandler.HandlerFuture;
import com.datastax.driver.core.AbstractReconnectionHandlerTest.MockReconnectionWork.ReconnectBehavior;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException;
import com.datastax.driver.core.policies.ReconnectionPolicy.ReconnectionSchedule;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.ConditionChecker.check;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.testng.Assert.fail;

public class AbstractReconnectionHandlerTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractReconnectionHandlerTest.class);

    ScheduledExecutorService executor;
    MockReconnectionSchedule schedule;
    MockReconnectionWork work;
    AtomicReference<ListenableFuture<?>> future = new AtomicReference<ListenableFuture<?>>();
    AbstractReconnectionHandler handler;
    Callable<Boolean> nextTryAssigned = new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
            return handler.handlerFuture.nextTry != null;
        }
    };

    @BeforeMethod(groups = {"unit", "short"})
    public void setup() {
        executor = spy(Executors.newScheduledThreadPool(2));
        schedule = new MockReconnectionSchedule();
        work = new MockReconnectionWork();
        future.set(null);
        handler = new AbstractReconnectionHandler("test", executor, schedule, future) {
            @Override
            protected Connection tryReconnect() throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
                return work.tryReconnect();
            }

            @Override
            protected void onReconnection(Connection connection) {
                work.onReconnection();
            }
        };
    }

    @AfterMethod(groups = {"unit", "short"}, alwaysRun = true)
    public void tearDown() {
        if (future.get() != null)
            future.get().cancel(false);
        executor.shutdownNow();
    }

    @Test(groups = "unit")
    public void should_complete_if_first_reconnection_succeeds() {
        handler.start();

        assertThat(future.get()).isNotNull();
        assertThat(future.get().isDone()).isFalse();

        schedule.tick();
        work.nextReconnect = ReconnectBehavior.SUCCEED;
        work.tick();

        waitForCompletion();

        assertThat(work.success).isTrue();
        assertThat(work.tries).isEqualTo(1);
        assertThat(future.get()).isNull();
    }

    @Test(groups = "unit")
    public void should_retry_until_success() {
        handler.start();

        int simulatedErrors = 10;
        for (int i = 0; i < simulatedErrors; i++) {
            schedule.tick();
            work.nextReconnect = ReconnectBehavior.THROW_EXCEPTION;
            work.tick();
            assertThat(work.success).isFalse();
            assertThat(future.get().isDone()).isFalse();
        }

        schedule.tick();
        work.nextReconnect = ReconnectBehavior.SUCCEED;
        work.tick();

        waitForCompletion();

        assertThat(work.success).isTrue();
        assertThat(work.tries).isEqualTo(simulatedErrors + 1);
        assertThat(future.get()).isNull();
    }

    @Test(groups = "unit")
    public void should_stop_if_cancelled_before_first_attempt() {
        schedule.delay = 10 * 1000; // give ourselves time to cancel
        handler.start();
        schedule.tick();

        future.get().cancel(false);

        waitForCompletion();

        assertThat(work.success).isFalse();
        assertThat(work.tries).isEqualTo(0);
        assertThat(future.get().isCancelled()).isTrue();
    }

    @Test(groups = "short")
    public void should_stop_if_cancelled_between_attempts() {
        handler.start();

        // Wait for the initial schedule of a reconnect.
        verify(executor, timeout(10000)).schedule(handler, 0, TimeUnit.MILLISECONDS);

        // Force a failed reconnect.
        schedule.tick();
        work.nextReconnect = ReconnectBehavior.THROW_EXCEPTION;
        // Tick work, should trigger the barrier in tryReconnect.
        work.tick();

        // Tick schedule, should cause nextDelayMs to proceed, reconnect handler will call reschedule.
        schedule.delay = 3000;
        schedule.tick();

        // Ensure reconnect is scheduled (slight timing window after handling failed reconnect
        // and scheduling next reconnect).
        verify(executor, timeout(10000)).schedule(handler, schedule.delay, TimeUnit.MILLISECONDS);

        // Wait until nextTry is assigned after schedule completes.
        check().before(10000).that(nextTryAssigned).becomesTrue();

        future.get().cancel(false);

        // Should immediately return as the future was cancelled while the task was scheduled.
        waitForCompletion();

        assertThat(work.success).isFalse();
        // Should have had 1 failed attempt, no second attempt since cancelled.
        assertThat(work.tries).isEqualTo(1);

        // The future will be marked cancelled and thus not executed.
        ListenableFuture<?> currentAttempt = future.get();

        assertThat(currentAttempt).isInstanceOf(HandlerFuture.class);
        HandlerFuture handlerFuture = (HandlerFuture) currentAttempt;
        assertThat(handlerFuture.isCancelled());

        // The next try should also be cancelled.
        assertThat(handlerFuture.nextTry).isNotNull();
        assertThat(handlerFuture.nextTry.isCancelled());
    }

    @Test(groups = "unit")
    public void should_complete_if_cancelled_during_successful_reconnect() throws InterruptedException {
        handler.start();

        schedule.tick();
        work.nextReconnect = ReconnectBehavior.SUCCEED;

        // short pause to make sure we are in the middle of the handler's run method (it checks
        // if the future is cancelled at the beginning)
        TimeUnit.MILLISECONDS.sleep(100);
        // don't force interruption because that's what the production code does
        future.get().cancel(false);

        work.tick();

        waitForCompletion();

        assertThat(work.success).isTrue();
        assertThat(work.tries).isEqualTo(1);
    }

    @Test(groups = "unit")
    public void should_stop_if_cancelled_during_failed_reconnect() throws InterruptedException {
        handler.start();

        schedule.tick();
        work.nextReconnect = ReconnectBehavior.THROW_EXCEPTION;

        // short pause to make sure we are in the middle of the handler's run method (it checks
        // if the future is cancelled at the beginning)
        TimeUnit.MILLISECONDS.sleep(100);
        // don't force interruption because that's what the production code does
        future.get().cancel(false);

        work.tick();

        // Need to
        schedule.tick();

        waitForCompletion();

        assertThat(work.success).isFalse();
        assertThat(work.tries).isEqualTo(1);
    }

    @Test(groups = "unit")
    public void should_yield_to_another_running_handler() {
        // Set an uncompleted future, representing a running handler
        future.set(SettableFuture.create());

        handler.start();

        // Increase the delay to make sure that the first attempt does not start before the check
        // for cancellation (which would require calling work.tick())
        schedule.delay = 5000;
        schedule.tick();

        waitForCompletion();

        assertThat(work.success).isFalse();
    }

    /**
     * Note: a handler that succeeds immediately resets the future to null, so there is a very small window of opportunity
     * for this scenario. Therefore we consider that if we find a completed future, the connection was successfully
     * re-established a few milliseconds ago, so we don't start another attempt.
     */
    @Test(groups = "unit")
    public void should_yield_to_another_handler_that_just_succeeded() {
        future.set(Futures.immediateCheckedFuture(null));

        handler.start();

        schedule.tick();

        waitForCompletion();

        assertThat(work.success).isFalse();
    }

    @Test(groups = "unit")
    public void should_run_if_another_handler_was_cancelled() {
        future.set(Futures.immediateCancelledFuture());

        handler.start();

        schedule.tick();
        work.nextReconnect = ReconnectBehavior.SUCCEED;
        work.tick();

        waitForCompletion();

        assertThat(work.success).isTrue();
        assertThat(work.tries).isEqualTo(1);
        assertThat(future.get()).isNull();
    }

    /**
     * A reconnection schedule that allows manually setting the delay.
     * <p/>
     * To make testing easier, nextDelay blocks until tick() is called from the main thread.
     */
    static class MockReconnectionSchedule implements ReconnectionSchedule {
        volatile long delay;
        private final CyclicBarrier barrier = new CyclicBarrier(2);

        // Hack to work around the fact that the first call to nextDelayMs is synchronous
        private volatile boolean firstDelay = true;
        private volatile boolean firstTick = true;

        @Override
        public long nextDelayMs() {
            if (firstDelay)
                firstDelay = false;
            else {
                logger.debug("in schedule, waiting for tick from main thread");
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    logger.debug("in schedule, got tick from main thread, proceeding");
                } catch (Exception e) {
                    fail("Error while waiting for tick", e);
                }
            }
            logger.debug("in schedule, returning {}", delay);
            return delay;
        }

        public void tick() {
            if (firstTick)
                firstTick = false;
            else {
                logger.debug("send tick to schedule");
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    fail("Error while sending tick, no thread was waiting", e);
                }
                barrier.reset();
            }
        }
    }

    /**
     * Simulates the work done by the overridable methods of the handler.
     * <p/>
     * Allows choosing whether the next reconnect will succeed or throw an exception.
     * To make testing easier, tryReconnect blocks until tick() is called from the main thread.
     */
    static class MockReconnectionWork {
        enum ReconnectBehavior {
            SUCCEED, THROW_EXCEPTION
        }

        private final CyclicBarrier barrier = new CyclicBarrier(2);

        volatile ReconnectBehavior nextReconnect;

        volatile int tries = 0;
        volatile boolean success = false;

        protected Connection tryReconnect() throws ConnectionException {
            tries += 1;
            logger.debug("in reconnection work, wait for tick from main thread");
            try {
                barrier.await(60, TimeUnit.SECONDS);
                logger.debug("in reconnection work, got tick from main thread, proceeding");
            } catch (Exception e) {
                fail("Error while waiting for tick", e);
            }
            switch (nextReconnect) {
                case SUCCEED:
                    logger.debug("simulate reconnection success");
                    return null;
                case THROW_EXCEPTION:
                    logger.debug("simulate reconnection error");
                    throw new ConnectionException(new InetSocketAddress(8888),
                            "Simulated exception from mock reconnection");
                default:
                    throw new AssertionError();
            }
        }

        public void tick() {
            logger.debug("send tick to reconnection work");
            try {
                barrier.await(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                fail("Error while sending tick, no thread was waiting", e);
            }
            barrier.reset();
        }

        protected void onReconnection() {
            success = true;
        }
    }

    private void waitForCompletion() {
        executor.shutdown();
        try {
            boolean shutdown = executor.awaitTermination(30, TimeUnit.SECONDS);
            if (!shutdown)
                fail("executor ran for longer than expected");
        } catch (InterruptedException e) {
            fail("Interrupted while waiting for executor to shutdown");
        }
    }
}
