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

import java.nio.ByteBuffer;

/**
 * A simple {@code Statement} implementation built directly from a query
 * string.
 */
public class SimpleStatement extends Statement {

    private final String query;
    private volatile ByteBuffer routingKey;

    /**
     * Creates a new {@code SimpleStatement} with the provided query string.
     *
     * @param query the query string.
     */
    public SimpleStatement(String query) {
        this.query = query;
    }

    /**
     * The query string.
     *
     * @return the query string;
     */
    public String getQueryString() {
        return query;
    }

    /**
     * The routing key for the query.
     * <p>
     * Note that unless the routing key has been explicitly set through
     * {@link #setRoutingKey}, this will method will return {@code null} (to
     * avoid having to parse the query string to retrieve the partition key).
     *
     * @return the routing key set through {@link #setRoutingKey} is such a key
     * was set, {@code null} otherwise.
     *
     * @see Query#getRoutingKey
     */
    public ByteBuffer getRoutingKey() {
        return routingKey;
    }

    /**
     * Set the routing key for this query.
     * <p>
     * This method allows to manually provide a routing key for this query. It
     * is thus optional since the routing key is only an hint for token aware
     * load balancing policy but is never mandatory.
     * <p>
     * If the partition key for the query is composite, use the
     * {@link #setRoutingKey(ByteBuffer...)} method instead to build the
     * routing key.
     *
     * @param routingKey the raw (binary) value to use as routing key.
     * @return this {@code SimpleStatement} object.
     *
     * @see Query#getRoutingKey
     */
    public SimpleStatement setRoutingKey(ByteBuffer routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    /**
     * Set the routing key for this query.
     * <p>
     * See {@link #setRoutingKey(ByteBuffer)} for more information. This
     * method is a variant for when the query partition key is composite and
     * thus the routing key must be built from multiple values.
     *
     * @param routingKeyComponents the raw (binary) values to compose to obtain
     * the routing key.
     * @return this {@code SimpleStatement} object.
     *
     * @see Query#getRoutingKey
     */
    public SimpleStatement setRoutingKey(ByteBuffer... routingKeyComponents) {
        this.routingKey = compose(routingKeyComponents);
        return this;
    }

    // TODO: we could find that a better place (but it's not expose so it doesn't matter too much)
    static ByteBuffer compose(ByteBuffer... buffers) {
        int totalLength = 0;
        for (ByteBuffer bb : buffers)
            totalLength += 2 + bb.remaining() + 1;

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer bb : buffers)
        {
            putShortLength(out, bb.remaining());
            out.put(bb);
            out.put((byte) 0);
        }
        out.flip();
        return out;
    }

    private static void putShortLength(ByteBuffer bb, int length) {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }
}
