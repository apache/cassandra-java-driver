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

import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple {@code RegularStatement} implementation built directly from a query
 * string.
 */
public class SimpleStatement extends RegularStatement {

    private final String query;
    private final Object[] values;
    private final Map<String, Object> namedValues;

    private volatile ByteBuffer routingKey;
    private volatile String keyspace;

    /**
     * Creates a new {@code SimpleStatement} with the provided query string (and no values).
     *
     * @param query the query string.
     */
    public SimpleStatement(String query) {
        this(query, (Object[]) null);
    }

    /**
     * Creates a new {@code SimpleStatement} with the provided query string and values.
     * <p/>
     * This version of SimpleStatement is useful when you want to execute a
     * query only once (and thus do not want to resort to prepared statement), but
     * do not want to convert all column values to string (typically, if you have blob
     * values, encoding them to a hexadecimal string is not very efficient). In
     * that case, you can provide a query string with bind markers to this constructor
     * along with the values for those bind variables. When executed, the server will
     * prepare the provided, bind the provided values to that prepare statement and
     * execute the resulting statement. Thus,
     * <pre>
     *   session.execute(new SimpleStatement(query, value1, value2, value3));
     * </pre>
     * is functionally equivalent to
     * <pre>
     *   PreparedStatement ps = session.prepare(query);
     *   session.execute(ps.bind(value1, value2, value3));
     * </pre>
     * except that the former version:
     * <ul>
     * <li>Requires only one round-trip to a Cassandra node.</li>
     * <li>Does not left any prepared statement stored in memory (neither client or
     * server side) once it has been executed.</li>
     * </ul>
     * <p/>
     * Note that the types of the {@code values} provided to this method will
     * not be validated by the driver as is done by {@link BoundStatement#bind} since
     * {@code query} is not parsed (and hence the driver cannot know what those values
     * should be). The codec to serialize each value will be chosen in the codec registry
     * associated with the cluster executing this statement, based on the value's Java type
     * (this is the equivalent to calling {@link CodecRegistry#codecFor(Object)}).
     * If too many or too few values are provided, or if a value is not a valid one for
     * the variable it is bound to, an
     * {@link com.datastax.driver.core.exceptions.InvalidQueryException} will be thrown
     * by Cassandra at execution time. A {@code CodecNotFoundException} may be
     * thrown by this constructor however, if the codec registry does not know how to
     * handle one of the values.
     * <p/>
     * If you have a single value of type {@code Map<String, Object>}, you can't call this
     * constructor using the varargs syntax, because the signature collides with
     * {@link #SimpleStatement(String, Map)}. To prevent this, pass an explicit
     * array object:
     * <pre>
     * new SimpleStatement("...", new Object[]{m});
     * </pre>
     *
     * @param query  the query string.
     * @param values values required for the execution of {@code query}.
     * @throws IllegalArgumentException if the number of values is greater than 65535.
     */
    public SimpleStatement(String query, Object... values) {
        if (values != null && values.length > 65535)
            throw new IllegalArgumentException("Too many values, the maximum allowed is 65535");
        this.query = query;
        this.values = values;
        this.namedValues = null;
    }

    /**
     * Creates a new {@code SimpleStatement} with the provided query string and named values.
     * <p/>
     * This constructor requires that the query string use named placeholders, for example:
     * <pre>{@code
     * new SimpleStatement("SELECT * FROM users WHERE id = :i", ImmutableMap.<String, Object>of("i", 1));}
     * </pre>
     * Make sure that the map is correctly typed {@code Map<String, Object>}, otherwise you might
     * accidentally call {@link #SimpleStatement(String, Object...)} with a positional value of type map.
     * <p/>
     * The types of the values will be handled the same way as with anonymous placeholders (see
     * {@link #SimpleStatement(String, Object...)}).
     * <p/>
     * Simple statements with named values are only supported starting with native protocol
     * {@link ProtocolVersion#V3 v3}. With earlier versions, an
     * {@link com.datastax.driver.core.exceptions.UnsupportedFeatureException} will be thrown at execution time.
     *
     * @param query  the query string.
     * @param values named values required for the execution of {@code query}.
     * @throws IllegalArgumentException if the number of values is greater than 65535.
     */
    public SimpleStatement(String query, Map<String, Object> values) {
        if (values.size() > 65535)
            throw new IllegalArgumentException("Too many values, the maximum allowed is 65535");
        this.query = query;
        this.values = null;
        this.namedValues = values;
    }

    @Override
    public String getQueryString(CodecRegistry codecRegistry) {
        return query;
    }

    @Override
    public ByteBuffer[] getValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        if (values == null)
            return null;
        return convert(values, protocolVersion, codecRegistry);
    }

    @Override
    public Map<String, ByteBuffer> getNamedValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        if (namedValues == null)
            return null;
        return convert(namedValues, protocolVersion, codecRegistry);
    }

    /**
     * The number of values for this statement, that is the size of the array
     * that will be returned by {@code getValues}.
     *
     * @return the number of values.
     */
    public int valuesCount() {
        if (values != null)
            return values.length;
        else if (namedValues != null)
            return namedValues.size();
        else
            return 0;
    }

    @Override
    public boolean hasValues(CodecRegistry codecRegistry) {
        return (values != null && values.length > 0)
                || (namedValues != null && namedValues.size() > 0);
    }

    @Override
    public boolean usesNamedValues() {
        return namedValues != null && namedValues.size() > 0;
    }

    /**
     * Returns the {@code i}th value as the Java type matching its CQL type.
     *
     * @param i the index to retrieve.
     * @return the value of the {@code i}th value of this statement.
     * @throws IllegalStateException     if this statement does not have values.
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
     * <p/>
     * Unless the routing key has been explicitly set through
     * {@link #setRoutingKey}, this method will return {@code null} to
     * avoid having to parse the query string to retrieve the partition key.
     *
     * @param protocolVersion unused by this implementation (no internal serialization is required to compute the key).
     * @param codecRegistry   unused by this implementation (no internal serialization is required to compute the key).
     * @return the routing key set through {@link #setRoutingKey} if such a key
     * was set, {@code null} otherwise.
     * @see Statement#getRoutingKey
     */
    @Override
    public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        return routingKey;
    }

    /**
     * Sets the routing key for this query.
     * <p/>
     * This method allows you to manually provide a routing key for this query. It
     * is thus optional since the routing key is only an hint for token aware
     * load balancing policy but is never mandatory.
     * <p/>
     * If the partition key for the query is composite, use the
     * {@link #setRoutingKey(ByteBuffer...)} method instead to build the
     * routing key.
     *
     * @param routingKey the raw (binary) value to use as routing key.
     * @return this {@code SimpleStatement} object.
     * @see Statement#getRoutingKey
     */
    public SimpleStatement setRoutingKey(ByteBuffer routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    /**
     * Returns the keyspace this query operates on.
     * <p/>
     * Unless the keyspace has been explicitly set through {@link #setKeyspace},
     * this method will return {@code null} to avoid having to parse the query
     * string.
     *
     * @return the keyspace set through {@link #setKeyspace} if such keyspace was
     * set, {@code null} otherwise.
     * @see Statement#getKeyspace
     */
    @Override
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Sets the keyspace this query operates on.
     * <p/>
     * This method allows you to manually provide a keyspace for this query. It
     * is thus optional since the value returned by this method is only an hint
     * for token aware load balancing policy but is never mandatory.
     * <p/>
     * Do note that if the query does not use a fully qualified keyspace, then
     * you do not need to set the keyspace through that method as the
     * currently logged in keyspace will be used.
     *
     * @param keyspace the name of the keyspace this query operates on.
     * @return this {@code SimpleStatement} object.
     * @see Statement#getKeyspace
     */
    public SimpleStatement setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Sets the routing key for this query.
     * <p/>
     * See {@link #setRoutingKey(ByteBuffer)} for more information. This
     * method is a variant for when the query partition key is composite and
     * thus the routing key must be built from multiple values.
     *
     * @param routingKeyComponents the raw (binary) values to compose to obtain
     *                             the routing key.
     * @return this {@code SimpleStatement} object.
     * @see Statement#getRoutingKey
     */
    public SimpleStatement setRoutingKey(ByteBuffer... routingKeyComponents) {
        this.routingKey = compose(routingKeyComponents);
        return this;
    }

    /*
     * This method performs a best-effort heuristic to guess which codec to use.
     * Note that this is not particularly efficient as the codec registry needs to iterate over
     * the registered codecs until it finds a suitable one.
     */
    private static ByteBuffer[] convert(Object[] values, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
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
                    serializedValues[i] = ((Token) value).serialize(protocolVersion);
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

    private static Map<String, ByteBuffer> convert(Map<String, Object> values, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        Map<String, ByteBuffer> serializedValues = new HashMap<String, ByteBuffer>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            if (value == null) {
                // impossible to locate the right codec when object is null,
                // so forcing the result to null
                serializedValues.put(name, null);
            } else {
                if (value instanceof Token) {
                    // bypass CodecRegistry for Token instances
                    serializedValues.put(name, ((Token) value).serialize(protocolVersion));
                } else {
                    try {
                        TypeCodec<Object> codec = codecRegistry.codecFor(value);
                        serializedValues.put(name, codec.serialize(value, protocolVersion));
                    } catch (Exception e) {
                        // Catch and rethrow to provide a more helpful error message (one that include which value is bad)
                        throw new InvalidTypeException(String.format("Value '%s' of type %s does not correspond to any CQL3 type", name, value.getClass()), e);
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
            out.put((byte) 0);
        }
        out.flip();
        return out;
    }

    static void putShortLength(ByteBuffer bb, int length) {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }

}
