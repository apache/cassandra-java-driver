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

import java.nio.ByteBuffer;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * A simple {@code RegularStatement} implementation built directly from a query
 * string.
 * <p>
 * Use {@link Session#newSimpleStatement(String)} to create a new instance of this
 * class.
 */
public class SimpleStatement extends RegularStatement {

    private final String query;
    private final Object[] values;
    private final Cluster cluster;

    private volatile ByteBuffer routingKey;
    private volatile String keyspace;
    
    protected SimpleStatement(String query, Cluster cluster) {
        this(query, cluster, (Object[]) null);
    }

    protected SimpleStatement(String query, Cluster cluster, Object... values) {
        if (values != null && values.length > 65535)
            throw new IllegalArgumentException("Too many values, the maximum allowed is 65535");
        this.query = query;
        this.values = values;
        this.cluster = cluster;
    }

    @Override
    public String getQueryString() {
        return query;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: Calling this method may trigger the underlying {@link Cluster} initialization.
     */
    @Override
    public ByteBuffer[] getValues() {
        if(values == null)
            return null;
        cluster.init();
        Configuration configuration = cluster.getConfiguration();
        ProtocolVersion protocolVersion = configuration.getProtocolOptions().getProtocolVersion();
        CodecRegistry codecRegistry = configuration.getCodecRegistry();
        return convert(values, protocolVersion, codecRegistry);
    }

    /**
     * The number of values for this statement, that is the size of the array
     * that will be returned by {@code getValues}.
     *
     * @return the number of values.
     */
    public int valuesCount() {
        return values == null ? 0 : values.length;
    }

    @Override
    public boolean hasValues() {
        return values != null && values.length > 0;
    }
    
    /**
     * Returns the {@code i}th value as the Java type matching its CQL type.
     *
     * @param i the index to retrieve.
     * @return the value of the {@code i}th value of this statement.
     *
     * @throws IllegalStateException if this statement does not have values.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public Object getObject(int i) {
        if (values == null)
            throw new IllegalStateException("This statement does not have values");
        if (i < 0 || i >= values.length)
            throw new ArrayIndexOutOfBoundsException(i);
        return values[i];
    }

    /**
     * Returns the routing key for the query.
     * <p>
     * Unless the routing key has been explicitly set through
     * {@link #setRoutingKey}, this method will return {@code null} to
     * avoid having to parse the query string to retrieve the partition key.
     *
     * @return the routing key set through {@link #setRoutingKey} if such a key
     * was set, {@code null} otherwise.
     *
     * @see Statement#getRoutingKey
     */
    @Override
    public ByteBuffer getRoutingKey() {
        return routingKey;
    }

    /**
     * Sets the routing key for this query.
     * <p>
     * This method allows you to manually provide a routing key for this query. It
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
     * @see Statement#getRoutingKey
     */
    public SimpleStatement setRoutingKey(ByteBuffer routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    /**
     * Returns the keyspace this query operates on.
     * <p>
     * Unless the keyspace has been explicitly set through {@link #setKeyspace},
     * this method will return {@code null} to avoid having to parse the query
     * string.
     *
     * @return the keyspace set through {@link #setKeyspace} if such keyspace was
     * set, {@code null} otherwise.
     *
     * @see Statement#getKeyspace
     */
    @Override
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Sets the keyspace this query operates on.
     * <p>
     * This method allows you to manually provide a keyspace for this query. It
     * is thus optional since the value returned by this method is only an hint
     * for token aware load balancing policy but is never mandatory.
     * <p>
     * Do note that if the query does not use a fully qualified keyspace, then
     * you do not need to set the keyspace through that method as the
     * currently logged in keyspace will be used.
     *
     * @param keyspace the name of the keyspace this query operates on.
     * @return this {@code SimpleStatement} object.
     *
     * @see Statement#getKeyspace
     */
    public SimpleStatement setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Sets the routing key for this query.
     * <p>
     * See {@link #setRoutingKey(ByteBuffer)} for more information. This
     * method is a variant for when the query partition key is composite and
     * thus the routing key must be built from multiple values.
     *
     * @param routingKeyComponents the raw (binary) values to compose to obtain
     * the routing key.
     * @return this {@code SimpleStatement} object.
     *
     * @see Statement#getRoutingKey
     */
    public SimpleStatement setRoutingKey(ByteBuffer... routingKeyComponents) {
        this.routingKey = compose(routingKeyComponents);
        return this;
    }

    /**
     * <p>
     * Utility method to serialize user-provided values.
     * <p>
     * This method is useful in situations where there is no metadata available and the underlying CQL
     * type for the values is not known.
     * <p>
     * This situation happens when a {@link SimpleStatement}
     * or a {@link com.datastax.driver.core.querybuilder.BuiltStatement} (Query Builder) contain values;
     * in these places, the driver has no way to determine the right CQL type to use.
     * <p>
     * This method performs a best-effort heuristic to guess which codec to use.
     * Note that this is not particularly efficient as the codec registry needs to iterate over
     * the registered codecs until it finds a suitable one.
     *
     * @param values The values to convert.
     * @param protocolVersion The protocol version to use.
     * @param codecRegistry The {@link CodecRegistry} to use.
     * @return The converted values.
     */
    public static ByteBuffer[] convert(Object[] values, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        ByteBuffer[] serializedValues = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value == null) {
                // impossible to locate the right codec when object is null,
                // so forcing the result to null
                serializedValues[i] = null;
            } else {
                if (value instanceof Token) {
                    // bypass CodecRegistry for Token instances
                    serializedValues[i] = ((Token)value).serialize(protocolVersion);
                } else {
                    try {
                        TypeCodec<Object> codec = codecRegistry.codecFor(value);
                        serializedValues[i] = codec.serialize(value, protocolVersion);
                    } catch (Exception e) {
                        // Catch and rethrow to provide a more helpful error message (one that include which value is bad)
                        throw new InvalidTypeException(String.format("Value %d of type %s does not correspond to any CQL3 type", i, value.getClass()), e);
                    }
                }
            }
        }
        return serializedValues;
    }

    /**
     * Utility method to assemble different routing key components into a single {@link ByteBuffer}.
     * Mainly intended for statements that need to generate a routing key out of their current values.
     *
     * @param buffers the components of the routing key.
     * @return A ByteBuffer containing the serialized routing key
     */
    static ByteBuffer compose(ByteBuffer... buffers) {
        if (buffers.length == 1)
            return buffers[0];

        int totalLength = 0;
        for (ByteBuffer bb : buffers)
            totalLength += 2 + bb.remaining() + 1;

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer buffer : buffers) {
            ByteBuffer bb = buffer.duplicate();
            putShortLength(out, bb.remaining());
            out.put(bb);
            out.put((byte)0);
        }
        out.flip();
        return out;
    }

    static void putShortLength(ByteBuffer bb, int length) {
        bb.put((byte)((length >> 8) & 0xFF));
        bb.put((byte)(length & 0xFF));
    }

}
