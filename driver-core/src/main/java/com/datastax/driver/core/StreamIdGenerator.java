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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Manages a set of integer identifiers.
 * <p/>
 * Clients can borrow an id with {@link #next()}, and return it to the set with {@link #release(int)}.
 * It is guaranteed that a given id can't be borrowed by two clients at the same time.
 * This class is thread-safe and non-blocking.
 * <p/>
 * Implementation notes: we use an atomic long array where each bit represents an id. It is set to 1 if
 * the id is available, 0 otherwise. When looking for an id, we find a long that has remaining 1's and
 * pick the rightmost one.
 * To minimize the average time to find that long, we search the array in a round-robin fashion.
 */
class StreamIdGenerator {
    static final int MAX_STREAM_PER_CONNECTION_V2 = 128;
    static final int MAX_STREAM_PER_CONNECTION_V3 = 32768;
    private static final long MAX_UNSIGNED_LONG = -1L;

    static StreamIdGenerator newInstance(ProtocolVersion version) {
        return new StreamIdGenerator(streamIdSizeFor(version));
    }

    private static int streamIdSizeFor(ProtocolVersion version) {
        switch (version) {
            case V1:
            case V2:
                return 1;
            case V3:
            case V4:
            case V5:
                return 2;
            default:
                throw version.unsupported();
        }
    }

    private final AtomicLongArray bits;
    private final int maxIds;
    private final AtomicInteger offset;

    // If a query timeout, we'll stop waiting for it. However in that case, we
    // can't release/reuse the ID because we don't know if the response is lost
    // or will just come back to use sometimes in the future. In that case, we
    // just "mark" the fact that we have one less available ID and marked counts
    // how many marks we've put.
    private final AtomicInteger marked = new AtomicInteger(0);

    private StreamIdGenerator(int streamIdSizeInBytes) {
        // Stream IDs are signed and we only handle positive values
        // (negative stream IDs are for server side initiated streams).
        maxIds = 1 << (streamIdSizeInBytes * 8 - 1);

        // This is true for 1 byte = 128 streams, and therefore for any higher value
        assert maxIds % 64 == 0;

        // We use one bit in our array of longs to represent each stream ID.
        bits = new AtomicLongArray(maxIds / 64);

        // Initialize all bits to 1
        for (int i = 0; i < bits.length(); i++)
            bits.set(i, MAX_UNSIGNED_LONG);

        offset = new AtomicInteger(bits.length() - 1);
    }

    public int next() {
        int previousOffset, myOffset;
        do {
            previousOffset = offset.get();
            myOffset = (previousOffset + 1) % bits.length();
        } while (!offset.compareAndSet(previousOffset, myOffset));

        for (int i = 0; i < bits.length(); i++) {
            int j = (i + myOffset) % bits.length();

            int id = atomicGetAndSetFirstAvailable(j);
            if (id >= 0)
                return id + (64 * j);
        }
        return -1;
    }

    public void release(int streamId) {
        atomicClear(streamId / 64, streamId % 64);
    }

    public void mark(int streamId) {
        marked.incrementAndGet();
    }

    public void unmark(int streamId) {
        marked.decrementAndGet();
    }

    public int maxAvailableStreams() {
        return maxIds - marked.get();
    }

    // Returns >= 0 if found and set an id, -1 if no bits are available.
    private int atomicGetAndSetFirstAvailable(int idx) {
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

    private void atomicClear(int idx, int toClear) {
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
