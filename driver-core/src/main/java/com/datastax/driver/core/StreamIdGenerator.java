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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Handle assigning stream id to message.
 */
abstract class StreamIdGenerator {

    static final int MAX_STREAM_PER_CONNECTION_V2 = 128;
    static final int MAX_STREAM_PER_CONNECTION_V3 = 32768;

    static final long MAX_UNSIGNED_LONG = -1L;

    public static StreamIdGenerator forProtocolVersion(int protocolVersion) {
        switch (protocolVersion) {
            case 1:
            case 2:
                return new V2Gen();
            case 3:
                return new V3Gen();
            default:
                throw new IllegalArgumentException("invalid protocol version "+protocolVersion);
        }
    }

    public abstract int next() throws BusyConnectionException;
    public abstract void release(int streamId);
    public abstract void mark(int streamId);
    public abstract void unmark(int streamId);
    public abstract int maxAvailableStreams();

    static class V3Gen extends StreamIdGenerator {

        // TODO either find a better solution than 512 long values per connection or eliminate duplicate code

        // Stream IDs are two bytes long, signed and we only handle positive values
        // (negative stream IDs are for server side initiated streams). So we have
        // 32768 different stream IDs and 512 longs are neccessary.
        private final AtomicLongArray bits = new AtomicLongArray(512);

        // If a query timeout, we'll stop waiting for it. However in that case, we
        // can't release/reuse the ID because we don't know if the response is lost
        // or will just come back to use sometimes in the future. In that case, we
        // just "mark" the fact that we have one less available ID and marked counts
        // how many marks we've put.
        private final AtomicInteger marked = new AtomicInteger(0);

        private V3Gen() {
            for (int i=0;i<512;i++)
                bits.set(i, MAX_UNSIGNED_LONG);
        }

        @Override public int next() throws BusyConnectionException {
            for (int i=0;i<512;i++) {
                int id = atomicGetAndSetFirstAvailable(i);
                if (id >= 0)
                    return id + i * 64;
            }

            throw new BusyConnectionException();
        }

        @Override public void release(int streamId) {
            for (int i=0;i<512;i++) {
                if (streamId < 64) {
                    atomicClear(i, streamId);
                    break;
                }
                streamId -= 64;
            }
        }

        @Override public void mark(int streamId) {
            marked.incrementAndGet();
        }

        @Override public void unmark(int streamId) {
            marked.decrementAndGet();
        }

        @Override public int maxAvailableStreams() {
            return MAX_STREAM_PER_CONNECTION_V3 - marked.get();
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

    static class V2Gen extends StreamIdGenerator {

        // Stream IDs are one byte long, signed and we only handle positive values
        // (negative stream IDs are for server side initiated streams). So we have
        // 128 different stream IDs and two longs are enough.
        private final AtomicLongArray bits = new AtomicLongArray(2);

        // If a query timeout, we'll stop waiting for it. However in that case, we
        // can't release/reuse the ID because we don't know if the response is lost
        // or will just come back to use sometimes in the future. In that case, we
        // just "mark" the fact that we have one less available ID and marked counts
        // how many marks we've put.
        private final AtomicInteger marked = new AtomicInteger(0);

        private V2Gen() {
            bits.set(0, MAX_UNSIGNED_LONG);
            bits.set(1, MAX_UNSIGNED_LONG);
        }

        @Override public int next() throws BusyConnectionException {
            int id = atomicGetAndSetFirstAvailable(0);
            if (id >= 0)
                return id;

            id = atomicGetAndSetFirstAvailable(1);
            if (id >= 0)
                return 64 + id;

            throw new BusyConnectionException();
        }

        @Override public void release(int streamId) {
            if (streamId < 64) {
                atomicClear(0, streamId);
            } else {
                atomicClear(1, streamId - 64);
            }
        }

        @Override public void mark(int streamId) {
            marked.incrementAndGet();
        }

        @Override public void unmark(int streamId) {
            marked.decrementAndGet();
        }

        @Override public int maxAvailableStreams() {
            return MAX_STREAM_PER_CONNECTION_V2 - marked.get();
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
}
