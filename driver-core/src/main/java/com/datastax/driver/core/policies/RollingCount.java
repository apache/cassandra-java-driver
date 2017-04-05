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
package com.datastax.driver.core.policies;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A "rolling" count over a 1-minute sliding window.
 */
class RollingCount {
    // Divide the minute into 5-second intervals
    private static final long INTERVAL_SIZE = TimeUnit.SECONDS.toNanos(5);

    // A circular buffer containing the counts over the 12 previous intervals. Their sum is the count we're looking for.
    // If we're at t = 61s this would span [0,60[
    private final AtomicLongArray previousIntervals = new AtomicLongArray(12);
    // The interval we're currently recording events for. It hasn't completed yet, so it's not included in the count.
    // If we're at t = 61s this would span [60,65[
    // Note that we don't expect very high concurrency on RollingCount (it's used to count errors), so AtomicLong is
    // good enough here.
    private final AtomicLong currentInterval = new AtomicLong();
    // Other mutable state, grouped in an object for atomic updates
    private final AtomicReference<State> state;
    private final Clock clock;

    RollingCount(Clock clock) {
        this.state = new AtomicReference<State>(new State(clock.nanoTime()));
        this.clock = clock;
    }

    void increment() {
        add(1);
    }

    void add(long amount) {
        tickIfNecessary();
        currentInterval.addAndGet(amount);
    }

    long get() {
        tickIfNecessary();
        return state.get().totalCount;
    }

    private void tickIfNecessary() {
        State oldState = state.get();
        long newTick = clock.nanoTime();
        long age = newTick - oldState.lastTick;
        if (age >= INTERVAL_SIZE) {
            long currentCount = currentInterval.get();

            long newIntervalStartTick = newTick - age % INTERVAL_SIZE;
            long elapsedIntervals = Math.min(age / INTERVAL_SIZE, 12);
            int newOffset = (int) ((oldState.offset + elapsedIntervals) % 12);

            long newTotal;
            if (elapsedIntervals == 12) {
                // We wrapped around the circular buffer, all our values are stale
                // Don't mutate previousIntervals yet because this part of the code is still multi-threaded.
                newTotal = 0;
            } else {
                // Add the current interval that just completed
                newTotal = oldState.totalCount + currentCount;
                // Subtract all elapsed intervals: they're either idle ones, or the one at the old offset that we're
                // about to replace
                for (int i = 1; i <= elapsedIntervals; i++) {
                    newTotal -= previousIntervals.get((newOffset + 12 - i) % 12);
                }
            }

            State newState = new State(newIntervalStartTick, newOffset, newTotal);
            if (state.compareAndSet(oldState, newState)) {
                // Only one thread gets here, so we can now:
                // - reset the current count (don't use reset because other threads might already have started updating
                // it)
                currentInterval.addAndGet(-currentCount);
                // - store the interval that just completed (or clear it if we wrapped)
                previousIntervals.set(oldState.offset, elapsedIntervals < 12 ? currentCount : 0);
                // - clear any idle interval
                for (int i = 1; i < elapsedIntervals; i++) {
                    previousIntervals.set((newOffset + 12 - i) % 12, 0);
                }
            }
        }
    }

    static class State {
        final long lastTick; // last time the state was modified
        final int offset; // the offset that the current interval will replace once it's complete
        final long totalCount; // cache of sum(previousIntervals)

        State(long lastTick) {
            this(lastTick, 0, 0);
        }

        State(long lastTick, int offset, long totalCount) {
            this.lastTick = lastTick;
            this.offset = offset;
            this.totalCount = totalCount;
        }
    }
}
