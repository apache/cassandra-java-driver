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

import com.datastax.driver.core.utils.MoreFutures;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A helper class to debounce events received by the Control Connection.
 * <p/>
 * This class accumulates received events, and delivers them when either:
 * - no events have been received for delayMs
 * - maxPendingEvents have been received
 */
class EventDebouncer<T> {

    private static final Logger logger = LoggerFactory.getLogger(EventDebouncer.class);

    static final int DEFAULT_MAX_QUEUED_EVENTS = 10000;

    private final String name;

    private final AtomicReference<DeliveryAttempt> immediateDelivery = new AtomicReference<DeliveryAttempt>(null);
    private final AtomicReference<DeliveryAttempt> delayedDelivery = new AtomicReference<DeliveryAttempt>(null);

    private final ScheduledExecutorService executor;

    private final DeliveryCallback<T> callback;

    private final long delayMs;
    private final int maxPendingEvents;
    private final int maxQueuedEvents;

    private final Queue<Entry<T>> events;
    private final AtomicInteger eventCount;

    private enum State {NEW, RUNNING, STOPPED}

    private volatile State state;

    private static final long OVERFLOW_WARNING_INTERVAL = NANOSECONDS.convert(5, SECONDS);
    private volatile long lastOverflowWarning = Long.MIN_VALUE;

    EventDebouncer(String name, ScheduledExecutorService executor, DeliveryCallback<T> callback, long delayMs, int maxPendingEvents, int maxQueuedEvents) {
        this.name = name;
        this.executor = executor;
        this.callback = callback;
        this.delayMs = delayMs;
        this.maxPendingEvents = maxPendingEvents;
        this.maxQueuedEvents = maxQueuedEvents;
        this.events = new ConcurrentLinkedQueue<Entry<T>>();
        this.eventCount = new AtomicInteger();
        this.state = State.NEW;
    }

    void start() {
        logger.trace("Starting {} debouncer...", name);
        state = State.RUNNING;
        if (!events.isEmpty()) {
            logger.trace("{} debouncer: {} events were accumulated before the debouncer started: delivering now",
                    name, eventCount.get());
            scheduleImmediateDelivery();
        }
    }

    void stop() {
        logger.trace("Stopping {} debouncer...", name);
        state = State.STOPPED;
        while (true) {
            DeliveryAttempt previous = cancelDelayedDelivery();
            if (delayedDelivery.compareAndSet(previous, null)) {
                break;
            }
        }

        completeAllPendingFutures();

        logger.trace("{} debouncer stopped", name);
    }

    private void completeAllPendingFutures() {
        Entry<T> entry;
        while ((entry = this.events.poll()) != null) {
            entry.future.set(null);
        }
    }

    /**
     * @return a future that will complete once the event has been processed
     */
    ListenableFuture<Void> eventReceived(T event) {
        if (state == State.STOPPED) {
            logger.trace("{} debouncer is stopped, rejecting event: {}", name, event);
            return MoreFutures.VOID_SUCCESS;
        }
        checkNotNull(event);
        logger.trace("{} debouncer: event received {}", name, event);

        // Safeguard against the queue filling up faster than we can process it
        if (eventCount.incrementAndGet() > maxQueuedEvents) {
            long now = System.nanoTime();
            if (now > lastOverflowWarning + OVERFLOW_WARNING_INTERVAL) {
                lastOverflowWarning = now;
                logger.warn("{} debouncer enqueued more than {} events, rejecting new events. "
                            + "This should not happen and is likely a sign that something is wrong.",
                        name, maxQueuedEvents);
            }
            eventCount.decrementAndGet();
            return MoreFutures.VOID_SUCCESS;
        }

        Entry<T> entry = new Entry<T>(event);
        try {
            events.add(entry);
        } catch (RuntimeException e) {
            eventCount.decrementAndGet();
            throw e;
        }

        if (state == State.RUNNING) {
            int count = eventCount.get();
            if (count < maxPendingEvents) {
                scheduleDelayedDelivery();
            } else if (count == maxPendingEvents) {
                scheduleImmediateDelivery();
            }
        } else if (state == State.STOPPED) {
            // If we race with stop() since the check at the beginning, ensure the future
            // gets completed (no-op if the future was already set).
            entry.future.set(null);
        }
        return entry.future;
    }

    private void scheduleImmediateDelivery() {
        cancelDelayedDelivery();

        while (state == State.RUNNING) {
            DeliveryAttempt previous = immediateDelivery.get();
            if (previous != null)
                previous.cancel();

            DeliveryAttempt current = new DeliveryAttempt();
            if (immediateDelivery.compareAndSet(previous, current)) {
                current.executeNow();
                return;
            }
        }
    }

    private void scheduleDelayedDelivery() {
        while (state == State.RUNNING) {
            DeliveryAttempt previous = cancelDelayedDelivery();
            DeliveryAttempt next = new DeliveryAttempt();
            if (delayedDelivery.compareAndSet(previous, next)) {
                next.scheduleAfterDelay();
                break;
            }
        }
    }

    private DeliveryAttempt cancelDelayedDelivery() {
        DeliveryAttempt previous = delayedDelivery.get();
        if (previous != null) {
            previous.cancel();
        }
        return previous;
    }

    void deliverEvents() {
        if (state == State.STOPPED) {
            completeAllPendingFutures();
            return;
        }
        final List<T> toDeliver = Lists.newArrayList();
        final List<SettableFuture<Void>> futures = Lists.newArrayList();

        Entry<T> entry;
        // Limit the number of events we dequeue, to avoid an infinite loop if the queue starts filling faster than we can consume it.
        int count = 0;
        while (++count <= maxQueuedEvents && (entry = this.events.poll()) != null) {
            toDeliver.add(entry.event);
            futures.add(entry.future);
        }
        eventCount.addAndGet(-toDeliver.size());

        if (toDeliver.isEmpty()) {
            logger.trace("{} debouncer: no events to deliver", name);
        } else {
            logger.trace("{} debouncer: delivering {} events", name, toDeliver.size());
            ListenableFuture<?> delivered = callback.deliver(toDeliver);
            Futures.addCallback(delivered, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object result) {
                    for (SettableFuture<Void> future : futures)
                        future.set(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    for (SettableFuture<Void> future : futures)
                        future.setException(t);
                }
            });
        }

        // If we didn't dequeue all events (or new ones arrived since we did), make sure we eventually
        // process the remaining events, because eventReceived might have skipped the delivery
        if (eventCount.get() > 0)
            scheduleDelayedDelivery();
    }

    class DeliveryAttempt extends ExceptionCatchingRunnable {

        volatile Future<?> deliveryFuture;

        boolean isDone() {
            return deliveryFuture != null && deliveryFuture.isDone();
        }

        void cancel() {
            if (deliveryFuture != null)
                deliveryFuture.cancel(true);
        }

        void executeNow() {
            if (state != State.STOPPED)
                deliveryFuture = executor.submit(this);
        }

        void scheduleAfterDelay() {
            if (state != State.STOPPED)
                deliveryFuture = executor.schedule(this, delayMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public void runMayThrow() throws Exception {
            deliverEvents();
        }
    }

    interface DeliveryCallback<T> {

        /**
         * Deliver the given list of events.
         * The given list is a private copy and any modification made to it
         * has no side-effect; it is also guaranteed not to be null nor empty.
         *
         * @param events the events to deliver
         */
        ListenableFuture<?> deliver(List<T> events);

    }

    static class Entry<T> {
        final T event;
        final SettableFuture<Void> future;

        Entry(T event) {
            this.event = event;
            this.future = SettableFuture.create();
        }
    }
}
