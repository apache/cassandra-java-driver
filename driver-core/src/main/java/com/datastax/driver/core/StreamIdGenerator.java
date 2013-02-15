/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Handle assigning stream id to message.
 */
class StreamIdGenerator {

    private static final long MAX_UNSIGNED_LONG = -1L;

    // Stream IDs are one byte long, signed and we only handle positive values
    // (negative stream IDs are for server side initiated streams). So we have
    // 128 different stream IDs and two longs are enough.
    private final AtomicLongArray bits = new AtomicLongArray(2);

    public StreamIdGenerator() {
        bits.set(0, MAX_UNSIGNED_LONG);
        bits.set(1, MAX_UNSIGNED_LONG);
    }

    public int next() throws BusyConnectionException {
        int id = atomicGetAndSetFirstAvailable(0);
        if (id >= 0)
            return id;

        id = atomicGetAndSetFirstAvailable(1);
        if (id >= 0)
            return 64 + id;

        throw new BusyConnectionException();
    }

    public void release(int streamId) {
        if (streamId < 64) {
            atomicClear(0, streamId);
        } else {
            atomicClear(1, streamId - 64);
        }
    }

    // Returns >= 0 if found and set an id, -1 if no bits are available.
    public int atomicGetAndSetFirstAvailable(int idx) {
        while (true) {
            long l = bits.get(idx);
            if (l == 0)
                return -1;

            // Find the position of the right-most 1-bit
            int id = Long.numberOfTrailingZeros(l);
            if (bits.compareAndSet(idx, l, l ^ mask(id)))
                return id;
        }
    }

    public void atomicClear(int idx, int toClear) {
        while (true) {
            long l = bits.get(idx);
            if (bits.compareAndSet(idx, l, l | mask(toClear)))
                return;
        }
    }

    private static long mask(int id) {
        return 1L << id;
    }
}
