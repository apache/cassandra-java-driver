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
import java.util.Arrays;
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
    private final List<Object> routingKeyValues;
    final String keyspace;
    private final ProtocolVersion protocolVersion;
    private final CodecRegistry codecRegistry;

    private boolean dirty;
    private String cache;
    private List<Object> values;
    Boolean isCounterOp;
    boolean hasNonIdempotentOps;

    // Whether the user has inputted bind markers. If that's the case, we never generate values as
    // it means the user meant for the statement to be prepared and we shouldn't add our own markers.
    boolean hasBindMarkers;
    private boolean forceNoValues;

    BuiltStatement(String keyspace, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        this.partitionKey = null;
        this.routingKeyValues = null;
        this.keyspace = keyspace;
        this.protocolVersion = protocolVersion;
        this.codecRegistry = codecRegistry;
    }

    BuiltStatement(TableMetadata tableMetadata, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        this.partitionKey = tableMetadata.getPartitionKey();
        this.routingKeyValues = Arrays.asList(new Object[tableMetadata.getPartitionKey().size()]);
        this.keyspace = escapeId(tableMetadata.getKeyspace().getName());
        this.protocolVersion = protocolVersion;
        this.codecRegistry = codecRegistry;
    }

    // Same as Metadata.escapeId, but we don't have access to it here.
    protected String escapeId(String ident) {
        // we don't need to escape if it's lowercase and match non-quoted CQL3 ids.
        return lowercaseId.matcher(ident).matches() ? ident : Metadata.quote(ident);
    }

    @Override
    public String getQueryString() {
        maybeRebuildCache();
        return cache;
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
        maybeRebuildCache();
        if (values == null || values.isEmpty())
            throw new IllegalStateException("This statement does not have values");
        if (i < 0 || i >= values.size())
            throw new ArrayIndexOutOfBoundsException(i);
        return values.get(i);
    }

    private void maybeRebuildCache() {
        if (!dirty && cache != null)
            return;

        StringBuilder sb;
        values = null;

        if (hasBindMarkers || forceNoValues) {
            sb = buildQueryString(null);
        } else {
            values = new ArrayList<Object>();
            sb = buildQueryString(values);

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

    abstract StringBuilder buildQueryString(List<Object> variables);

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

    // TODO: Correctly document the InvalidTypeException
    void maybeAddRoutingKey(String name, Object value) {
        if (routingKeyValues == null || name == null || value == null || Utils.containsSpecialValue(value))
            return;

        for (int i = 0; i < partitionKey.size(); i++) {
            if (name.equals(partitionKey.get(i).getName())) {
                routingKeyValues.set(i, value);
                return;
            }
        }
    }

    CodecRegistry getCodecRegistry(){
        return codecRegistry;
    }

    ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    @Override
    public ByteBuffer getRoutingKey() {
        if (routingKeyValues == null)
            return null;
        ByteBuffer[] routingKeyParts = new ByteBuffer[partitionKey.size()];
        CodecRegistry codecRegistry = getCodecRegistry();
        for (int i = 0; i < partitionKey.size(); i++) {
            Object value = routingKeyValues.get(i);
            if(value == null)
                return null;
            TypeCodec<Object> codec = codecRegistry.codecFor(partitionKey.get(i).getType(), value);
            routingKeyParts[i] = codec.serialize(value, getProtocolVersion());
        }
        return routingKeyParts.length == 1
            ? routingKeyParts[0]
            : compose(routingKeyParts);
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: Calling this method may trigger the underlying {@link Cluster} initialization.
     */
    @Override
    public ByteBuffer[] getValues() {
        maybeRebuildCache();
        return values == null ? null : convert(values.toArray(), getProtocolVersion(), getCodecRegistry());
    }

    @Override
    public boolean hasValues() {
        maybeRebuildCache();
        return values != null;
    }

    @Override
    public Boolean isIdempotent() {
        // If a value was forced with setIdempotent, it takes priority
        if (idempotent != null)
            return idempotent;

        // Otherwise return the computed value
        return !hasNonIdempotentOps();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: Calling this method may trigger the underlying {@link Cluster} initialization.
     */
    @Override
    public String toString() {
        if (forceNoValues) {
            return getQueryString();
        }
        return maybeAddSemicolon(buildQueryString(null)).toString();
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
            super((String)null, statement.getProtocolVersion(), statement.getCodecRegistry());
            this.statement = statement;
        }

        @Override
        public String getQueryString() {
            return statement.getQueryString();
        }

        @Override
        StringBuilder buildQueryString(List<Object> values) {
            return statement.buildQueryString(values);
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
