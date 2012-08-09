package com.datastax.driver.core.transport;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handle assigning stream id to message.
 */
public class StreamIdHandler {

    // Stream ids are one byte long, signed and we only handle positive values ourselves.
    private static final int STREAM_ID_COUNT = 128;

    // Keep one bit to know which one is in use.
    private final BitSet usedIds = new BitSet(STREAM_ID_COUNT);
    private final AtomicInteger idx = new AtomicInteger(0);

    public int next() {
        int next = idx.getAndIncrement() % STREAM_ID_COUNT;
        // Note: we could be fancier, and "search" for the next available idx,
        // though that could be race prone, so doing the simplest thing for now
        if (usedIds.get(next))
            // TODO: Throw a BusyConnectionException and handle it in the connection pool
            throw new IllegalStateException();
        return next;
    }

    public void release(int streamId) {
        usedIds.clear(streamId);
    }
}
