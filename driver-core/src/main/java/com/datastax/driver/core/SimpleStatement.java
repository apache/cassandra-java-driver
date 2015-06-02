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

/**
 * A simple {@code RegularStatement} implementation built directly from a query
 * string.
 */
public class SimpleStatement extends RegularStatement {

    private final String query;
    private final Object[] values;

    private volatile ByteBuffer routingKey;
    private volatile String keyspace;

    /**
     * Creates a new {@code SimpleStatement} with the provided query string (and no values).
     *
     * @param query the query string.
     */
    public SimpleStatement(String query) {
        this.query = query;
        this.values = null;
    }

    /**
     * Creates a new {@code SimpleStatement} with the provided query string and values.
     * <p>
     * This version of SimpleStatement is useful when you do not want to execute a
     * query only once (and thus do not want to resort to prepared statement), but
     * do not want to convert all column values to string (typically, if you have blob
     * values, encoding them to a hexadecimal string is not very efficient). In
     * that case, you can provide a query string with bind marker to this constructor
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
     *   <li>Requires only one round-trip to a Cassandra node.</li>
     *   <li>Does not left any prepared statement stored in memory (neither client or
     *   server side) once it has been executed.</li>
     * </ul>
     * <p>
     * Note that the type of the {@code values} provided to this method will
     * not be validated by the driver as is done by {@link BoundStatement#bind} since
     * {@code query} is not parsed (and hence the driver cannot know what those value
     * should be). If too much or too little values are provided or if a value is not
     * a valid one for the variable it is bound to, an
     * {@link com.datastax.driver.core.exceptions.InvalidQueryException} will be thrown
     * by Cassandra at execution time. An {@code IllegalArgumentException} may be
     * thrown by this constructor however if one of the value does not correspond to
     * any CQL3 type (for instance, if it is a custom class).
     *
     * @param query the query string.
     * @param values values required for the execution of {@code query}.
     *
     * @throws IllegalArgumentException if one of {@code values} is not of a type
     * corresponding to a CQL3 type, i.e. is not a Class that could be returned
     * by {@link DataType#asJavaClass}.
     */
    public SimpleStatement(String query, Object... values) {
        if (values.length > 65535)
            throw new IllegalArgumentException("Too many values, the maximum allowed is 65535");
        this.query = query;
        this.values = values;
    }

    private static ByteBuffer[] convert(Object[] values, ProtocolVersion protocolVersion) {
        ByteBuffer[] serializedValues = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            try {
                if (value instanceof Token)
                    value = ((Token)value).getValue();
                serializedValues[i] = DataType.serializeValue(value, protocolVersion);
            } catch (IllegalArgumentException e) {
                // Catch and rethrow to provide a more helpful error message (one that include which value is bad)
                throw new IllegalArgumentException(String.format("Value %d of type %s does not correspond to any CQL3 type", i, value.getClass()));
            }
        }
        return serializedValues;
    }

    /**
     * Returns the query string.
     *
     * @return the query string;
     */
    @Override
    public String getQueryString() {
        return query;
    }

    @Override
    public ByteBuffer[] getValues(ProtocolVersion protocolVersion) {
        return values == null ? null : convert(values, protocolVersion);
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

    // TODO: we could find that a better place (but it's not expose so it doesn't matter too much)
    static ByteBuffer compose(ByteBuffer... buffers) {
        int totalLength = 0;
        for (ByteBuffer bb : buffers)
            totalLength += 2 + bb.remaining() + 1;

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer buffer : buffers)
        {
            ByteBuffer bb = buffer.duplicate();
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