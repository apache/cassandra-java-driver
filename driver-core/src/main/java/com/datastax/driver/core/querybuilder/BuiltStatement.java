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
package com.datastax.driver.core.querybuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RetryPolicy;

import static com.datastax.driver.core.CodecUtils.compose;
import static com.datastax.driver.core.CodecUtils.convert;

/**
 * Common ancestor to the query builder built statements.
 */
public abstract class BuiltStatement extends RegularStatement {

    private static final Pattern lowercaseId = Pattern.compile("[a-z][a-z0-9_]*");

    private final List<ColumnMetadata> partitionKey;
    private final ByteBuffer[] routingKey;
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

    BuiltStatement(String keyspace) {
        this.partitionKey = null;
        this.routingKey = null;
        this.keyspace = keyspace;
    }

    BuiltStatement(TableMetadata tableMetadata) {
        this.partitionKey = tableMetadata.getPartitionKey();
        this.routingKey = new ByteBuffer[tableMetadata.getPartitionKey().size()];
        this.keyspace = escapeId(tableMetadata.getKeyspace().getName());
    }

    // Same as Metadata.escapeId, but we don't have access to it here.
    protected String escapeId(String ident) {
        // we don't need to escape if it's lowercase and match non-quoted CQL3 ids.
        return lowercaseId.matcher(ident).matches() ? ident : Metadata.quote(ident);
    }

     /**
     * Returns the query string for this statement.
     *
     * @deprecated for {@link BuiltStatement}s, use {@link #getQueryString(CodecRegistry)}
     * instead.
     * @return a valid CQL query string.
     */
    @Override
    @Deprecated
    public String getQueryString() {
        return getQueryString(CodecRegistry.DEFAULT_INSTANCE);
    }

    /**
     * Returns the query string for this statement.
     * If there are any values to be included in the resulting
     * string, these will be formatted using the given {@link CodecRegistry}.
     *
     * @param codecRegistry The {@link CodecRegistry} instance to use.
     * @return a valid CQL query string.
     */
    public String getQueryString(CodecRegistry codecRegistry) {
        maybeRebuildCache(codecRegistry);
        return cache;
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

    void maybeAddRoutingKey(String name, Object value) {
        if (routingKey == null || name == null || value == null || value instanceof BindMarker)
            return;

        // Always use default CodecRegistry instance for routing keys
        CodecRegistry codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
        for (int i = 0; i < partitionKey.size(); i++) {
            if (name.equals(partitionKey.get(i).getName()) && Utils.isRawValue(value)) {
                routingKey[i] = codecRegistry.codecFor(partitionKey.get(i).getType()).serialize(value);
                return;
            }
        }
    }

    @Override
    public ByteBuffer getRoutingKey() {
        if (routingKey == null)
            return null;

        for (ByteBuffer bb : routingKey)
            if (bb == null)
                return null;

        return routingKey.length == 1
             ? routingKey[0]
             : compose(routingKey);
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public ByteBuffer[] getValues() {
        return getValues(CodecRegistry.DEFAULT_INSTANCE);
    }

    @Override
    public ByteBuffer[] getValues(CodecRegistry codecRegistry) {
        maybeRebuildCache(codecRegistry);
        return values == null ? null : convert(values.toArray(), codecRegistry);
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
        if (forceNoValues)
            return getQueryString(CodecRegistry.DEFAULT_INSTANCE);

        return maybeAddSemicolon(buildQueryString(null, CodecRegistry.DEFAULT_INSTANCE)).toString();
    }

    /**
     * Allows to force this builder to not generate values (through its {@code getValues()} method).
     * <p>
     * By default (and unless the protocol version 1 is in use, see below) and
     * for performance reasons, the query builder will not serialize all values
     * provided to strings. This means that the {@link #getQueryString} may
     * return a query string with bind markers (where and when is at the
     * discretion of the builder) and {@link #getValues} will return the binary
     * values for those markers. This method allows to force the builder to not
     * generate binary values but rather to serialize them all in the query
     * string. In practice, this means that if you call {@code
     * setForceNoValues(true)}, you are guaranteed that {@code getValues()} will
     * return {@code null} and that the string returned by {@code
     * getQueryString()} will contain no other bind markers than the one
     * inputted by the user.
     * <p>
     * If the native protocol version 1 is in use, the driver will default
     * to not generating values since those are not supported by that version of
     * the protocol. In practice, the driver will automatically call this method
     * with {@code true} as argument prior to execution. Hence, calling this
     * method when the protocol version 1 is in use is basically a no-op.
     * <p>
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
            super((String)null);
            this.statement = statement;
        }

        @SuppressWarnings("deprecation")
        @Override
        public String getQueryString() {
            return statement.getQueryString();
        }

        @Override
        StringBuilder buildQueryString(List<Object> values, CodecRegistry codecRegistry) {
            return statement.buildQueryString(values, codecRegistry);
        }

        @Override
        public ByteBuffer getRoutingKey() {
            return statement.getRoutingKey();
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
        public ByteBuffer[] getValues() {
            return statement.getValues();
        }

        @Override
        public ByteBuffer[] getValues(CodecRegistry codecRegistry) {
            return statement.getValues(codecRegistry);
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
