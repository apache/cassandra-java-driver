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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.policies.RetryPolicy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Common ancestor to statements generated with the {@link QueryBuilder}.
 * <p/>
 * The actual query string will be generated and cached the first time it is requested,
 * which is either when the driver tries to execute the query, or when you call certain
 * public methods (for example {@link RegularStatement#getQueryString(CodecRegistry)},
 * {@link #getObject(int, CodecRegistry)}).
 * <p/>
 * Whenever possible (and unless you call {@link #setForceNoValues(boolean)}, the builder
 * will try to handle values passed to its methods as standalone values bound to the query
 * string with placeholders. For instance:
 * <pre>
 *     select().all().from("foo").where(eq("k", "the key"));
 *     // Is equivalent to:
 *     new SimpleStatement("SELECT * FROM foo WHERE k=?", "the key");
 * </pre>
 * There are a few exceptions to this rule:
 * <ul>
 * <li>for fixed-size number types, the builder can't guess what the actual CQL type
 * is. Standalone values are sent to Cassandra in their serialized form, and number
 * types aren't all serialized in the same way, so picking the wrong type could
 * lead to a query error;</li>
 * <li>if the value is a "special" element like a function call, it can't be serialized.
 * This also applies to collections mixing special elements and regular objects.</li>
 * </ul>
 * In these cases, the builder will inline the value in the query string:
 * <pre>
 *     select().all().from("foo").where(eq("k", 1));
 *     // Is equivalent to:
 *     new SimpleStatement("SELECT * FROM foo WHERE k=1");
 * </pre>
 * One final thing to consider is {@link CodecRegistry custom codecs}. If you've registered
 * codecs to handle your own Java types against the cluster, then you can pass instances of
 * those types to query builder methods. But should the builder have to inline those values,
 * it needs your codecs to {@link TypeCodec#format(Object) convert them to string form}.
 * That is why some of the public methods of this class take a {@code CodecRegistry} as a
 * parameter:
 * <pre>
 *     BuiltStatement s = select().all().from("foo").where(eq("k", myCustomObject));
 *     // if we do this codecs will definitely be needed:
 *     s.forceNoValues(true);
 *     s.getQueryString(myCodecRegistry);
 * </pre>
 * For convenience, there are no-arg versions of those methods that use
 * {@link CodecRegistry#DEFAULT_INSTANCE}. But you should only use them if you are sure that
 * no custom values will need to be inlined while building the statement, or if you have
 * registered your custom codecs with the default registry instance. Otherwise, you will get
 * a {@link CodecNotFoundException}.
 */
public abstract class BuiltStatement extends RegularStatement {

    private static final Pattern lowercaseAlphanumeric = Pattern.compile("[a-z][a-z0-9_]*");

    private final List<ColumnMetadata> partitionKey;
    private final List<Object> routingKeyValues;
    final String keyspace;

    private boolean dirty;
    private String cache;
    private List<Object> values;
    Boolean isCounterOp;
    boolean hasNonIdempotentOps;

    // Whether the user has inputted bind markers. If that's the case, we never generate values as
    // it means the user meant for the statement to be prepared and we shouldn't add our own markers.
    boolean hasBindMarkers;
    private boolean forceNoValues;

    BuiltStatement(String keyspace, List<ColumnMetadata> partitionKey, List<Object> routingKeyValues) {
        this.partitionKey = partitionKey;
        this.routingKeyValues = routingKeyValues;
        this.keyspace = keyspace;
    }

    /**
     * @deprecated preserved for backward compatibility, use {@link Metadata#quoteIfNecessary(String)} instead.
     */
    @Deprecated
    protected static String escapeId(String ident) {
        return Metadata.quoteIfNecessary(ident);
    }

    @Override
    public String getQueryString(CodecRegistry codecRegistry) {
        maybeRebuildCache(codecRegistry);
        return cache;
    }

    /**
     * Returns the {@code i}th value as the Java type matching its CQL type.
     *
     * @param i             the index to retrieve.
     * @param codecRegistry the codec registry that will be used if the statement must be
     *                      rebuilt in order to determine if it has values, and Java objects
     *                      must be inlined in the process (see {@link BuiltStatement} for
     *                      more explanations on why this is so).
     * @return the value of the {@code i}th value of this statement.
     * @throws IllegalStateException     if this statement does not have values.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @see #getObject(int)
     */
    public Object getObject(int i, CodecRegistry codecRegistry) {
        maybeRebuildCache(codecRegistry);
        if (values == null || values.isEmpty())
            throw new IllegalStateException("This statement does not have values");
        if (i < 0 || i >= values.size())
            throw new ArrayIndexOutOfBoundsException(i);
        return values.get(i);
    }

    /**
     * Returns the {@code i}th value as the Java type matching its CQL type.
     * <p/>
     * This method calls {@link #getObject(int, CodecRegistry)} with
     * {@link CodecRegistry#DEFAULT_INSTANCE}.
     * It's safe to use if you don't use any custom codecs, or if your custom codecs are in
     * the default registry; otherwise, use the other method and provide the registry that
     * contains your codecs.
     *
     * @param i the index to retrieve.
     * @return the value of the {@code i}th value of this statement.
     * @throws IllegalStateException     if this statement does not have values.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public Object getObject(int i) {
        return getObject(i, CodecRegistry.DEFAULT_INSTANCE);
    }

    private void maybeRebuildCache(CodecRegistry codecRegistry) {
        if (!dirty && cache != null)
            return;

        StringBuilder sb;
        values = null;

        if (hasBindMarkers || forceNoValues) {
            sb = buildQueryString(null, codecRegistry);
        } else {
            values = new ArrayList<Object>();
            sb = buildQueryString(values, codecRegistry);

            if (values.size() > 65535)
                throw new IllegalArgumentException("Too many values for built statement, the maximum allowed is 65535");

            if (values.isEmpty())
                values = null;
        }

        maybeAddSemicolon(sb);

        cache = sb.toString();
        dirty = false;
    }

    static StringBuilder maybeAddSemicolon(StringBuilder sb) {
        // Use the same test that String#trim() uses to determine
        // if a character is a whitespace character.
        int l = sb.length();
        while (l > 0 && sb.charAt(l - 1) <= ' ')
            l -= 1;
        if (l != sb.length())
            sb.setLength(l);

        if (l == 0 || sb.charAt(l - 1) != ';')
            sb.append(';');
        return sb;
    }

    abstract StringBuilder buildQueryString(List<Object> variables, CodecRegistry codecRegistry);

    boolean isCounterOp() {
        return isCounterOp == null ? false : isCounterOp;
    }

    void setCounterOp(boolean isCounterOp) {
        this.isCounterOp = isCounterOp;
    }

    boolean hasNonIdempotentOps() {
        return hasNonIdempotentOps;
    }

    void setNonIdempotentOps() {
        hasNonIdempotentOps = true;
    }

    void setDirty() {
        dirty = true;
    }

    void checkForBindMarkers(Object value) {
        dirty = true;
        if (Utils.containsBindMarker(value))
            hasBindMarkers = true;
    }

    void checkForBindMarkers(Utils.Appendeable value) {
        dirty = true;
        if (value != null && value.containsBindMarker())
            hasBindMarkers = true;
    }

    // TODO: Correctly document the InvalidTypeException
    void maybeAddRoutingKey(String name, Object value) {
        if (routingKeyValues == null || name == null || value == null || Utils.containsSpecialValue(value))
            return;

        for (int i = 0; i < partitionKey.size(); i++) {
            if (Utils.handleId(name).equals(partitionKey.get(i).getName())) {
                routingKeyValues.set(i, value);
                return;
            }
        }
    }

    @Override
    public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        if (routingKeyValues == null)
            return null;
        ByteBuffer[] routingKeyParts = new ByteBuffer[partitionKey.size()];
        for (int i = 0; i < partitionKey.size(); i++) {
            Object value = routingKeyValues.get(i);
            if (value == null)
                return null;
            TypeCodec<Object> codec = codecRegistry.codecFor(partitionKey.get(i).getType(), value);
            routingKeyParts[i] = codec.serialize(value, protocolVersion);
        }
        return routingKeyParts.length == 1
                ? routingKeyParts[0]
                : Utils.compose(routingKeyParts);
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public ByteBuffer[] getValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        maybeRebuildCache(codecRegistry);
        return values == null ? null : Utils.convert(values.toArray(), protocolVersion, codecRegistry);
    }

    @Override
    public boolean hasValues(CodecRegistry codecRegistry) {
        maybeRebuildCache(codecRegistry);
        return values != null;
    }

    @Override
    public Map<String, ByteBuffer> getNamedValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        // Built statements never return named values
        return null;
    }

    @Override
    public boolean usesNamedValues() {
        return false;
    }

    @Override
    public Boolean isIdempotent() {
        // If a value was forced with setIdempotent, it takes priority
        if (idempotent != null)
            return idempotent;

        // Otherwise return the computed value
        return !hasNonIdempotentOps();
    }

    @Override
    public String toString() {
        try {
            if (forceNoValues)
                return getQueryString();
            // 1) try first with all values inlined (will not work if some values require custom codecs,
            // or if the required codecs are registered in a different CodecRegistry instance than the default one)
            return maybeAddSemicolon(buildQueryString(null, CodecRegistry.DEFAULT_INSTANCE)).toString();
        } catch (RuntimeException e1) {
            // 2) try next with bind markers for all values to avoid usage of custom codecs
            try {
                return maybeAddSemicolon(buildQueryString(new ArrayList<Object>(), CodecRegistry.DEFAULT_INSTANCE)).toString();
            } catch (RuntimeException e2) {
                // Ugly but we have absolutely no context to get the registry from
                return String.format("built query (could not generate with default codec registry: %s)", e2.getMessage());
            }
        }
    }

    /**
     * Allows to force this builder to not generate values (through its {@code getValues()} method).
     * <p/>
     * By default (and unless the protocol version 1 is in use, see below) and
     * for performance reasons, the query builder will not serialize all values
     * provided to strings. This means that {@link #getQueryString(CodecRegistry)}
     * may return a query string with bind markers (where and when is at the
     * discretion of the builder) and {@link #getValues} will return the binary
     * values for those markers. This method allows to force the builder to not
     * generate binary values but rather to inline them all in the query
     * string. In practice, this means that if you call {@code
     * setForceNoValues(true)}, you are guaranteed that {@code getValues()} will
     * return {@code null} and that the string returned by {@code
     * getQueryString()} will contain no other bind markers than the ones
     * specified by the user.
     * <p/>
     * If the native protocol version 1 is in use, the driver will default
     * to not generating values since those are not supported by that version of
     * the protocol. In practice, the driver will automatically call this method
     * with {@code true} as argument prior to execution. Hence, calling this
     * method when the protocol version 1 is in use is basically a no-op.
     * <p/>
     * Note that this method is mainly useful for debugging purpose. In general,
     * the default behavior should be the correct and most efficient one.
     *
     * @param forceNoValues whether or not this builder may generate values.
     * @return this statement.
     */
    public RegularStatement setForceNoValues(boolean forceNoValues) {
        this.forceNoValues = forceNoValues;
        this.dirty = true;
        return this;
    }

    /**
     * An utility class to create a BuiltStatement that encapsulate another one.
     */
    abstract static class ForwardingStatement<T extends BuiltStatement> extends BuiltStatement {

        T statement;

        ForwardingStatement(T statement) {
            super(null, null, null);
            this.statement = statement;
        }

        @Override
        public String getQueryString(CodecRegistry codecRegistry) {
            return statement.getQueryString(codecRegistry);
        }

        @Override
        StringBuilder buildQueryString(List<Object> values, CodecRegistry codecRegistry) {
            return statement.buildQueryString(values, codecRegistry);
        }

        @Override
        public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
            return statement.getRoutingKey(protocolVersion, codecRegistry);
        }

        @Override
        public String getKeyspace() {
            return statement.getKeyspace();
        }

        @Override
        boolean isCounterOp() {
            return statement.isCounterOp();
        }

        @Override
        boolean hasNonIdempotentOps() {
            return statement.hasNonIdempotentOps();
        }

        @Override
        public RegularStatement setForceNoValues(boolean forceNoValues) {
            statement.setForceNoValues(forceNoValues);
            return this;
        }

        @Override
        public Statement setConsistencyLevel(ConsistencyLevel consistency) {
            statement.setConsistencyLevel(consistency);
            return this;
        }

        @Override
        public ConsistencyLevel getConsistencyLevel() {
            return statement.getConsistencyLevel();
        }

        @Override
        public Statement enableTracing() {
            statement.enableTracing();
            return this;
        }

        @Override
        public Statement disableTracing() {
            statement.disableTracing();
            return this;
        }

        @Override
        public boolean isTracing() {
            return statement.isTracing();
        }

        @Override
        public Statement setRetryPolicy(RetryPolicy policy) {
            statement.setRetryPolicy(policy);
            return this;
        }

        @Override
        public RetryPolicy getRetryPolicy() {
            return statement.getRetryPolicy();
        }

        @Override
        public ByteBuffer[] getValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
            return statement.getValues(protocolVersion, codecRegistry);
        }

        @Override
        public boolean hasValues() {
            return statement.hasValues();
        }

        @Override
        void checkForBindMarkers(Object value) {
            statement.checkForBindMarkers(value);
        }

        @Override
        void checkForBindMarkers(Utils.Appendeable value) {
            statement.checkForBindMarkers(value);
        }

        @Override
        public String toString() {
            return statement.toString();
        }
    }
}
