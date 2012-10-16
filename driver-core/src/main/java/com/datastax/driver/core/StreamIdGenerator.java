package com.datastax.driver.core;

import java.util.BitSet;
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

    public int next() {
        int id = atomicGetAndSetFirstAvailable(0);
        if (id >= 0)
            return id;

        id = atomicGetAndSetFirstAvailable(1);
        if (id >= 0)
            return 64 + id;

        // TODO: Throw a BusyConnectionException and handle it in the connection pool
        throw new IllegalStateException();
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
            int id = Long.numberOfTrailingZeros(Long.lowestOneBit(l));
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
