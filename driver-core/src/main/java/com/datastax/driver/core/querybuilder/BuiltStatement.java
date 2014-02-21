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
package com.datastax.driver.core.querybuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Common ancestor to the query builder built statements.
 */
public abstract class BuiltStatement extends RegularStatement {

    private static final Pattern cqlId = Pattern.compile("\\w+");

    private final List<ColumnMetadata> partitionKey;
    private final ByteBuffer[] routingKey;
    final String keyspace;

    private boolean dirty;
    private String cache;
    private ByteBuffer[] values;

    Boolean isCounterOp;

    // Whether the user has inputed bind markers. If that's the case, we never generate values as
    // it means the user meant for the statement to be prepared and we shouldn't add our own markers.
    boolean hasBindMarkers;
    private boolean forceNoValues;

    BuiltStatement(String keyspace) {
        this.partitionKey = null;
        this.routingKey = null;
        this.keyspace = handleId(keyspace);
    }

    // Same as in Metadata, but we don't want to expose it publicly there.
    private static String handleId(String id) {
        if (id == null)
            return null;

        if (cqlId.matcher(id).matches())
            return id.toLowerCase();

        // Check if it's enclosed in quotes. If it is, remove them
        if (id.charAt(0) == '"' && id.charAt(id.length() - 1) == '"')
            return id.substring(1, id.length() - 1);

        // otherwise, just return the id.
        return id;
    }

    BuiltStatement(TableMetadata tableMetadata) {
        this.partitionKey = tableMetadata.getPartitionKey();
        this.routingKey = new ByteBuffer[tableMetadata.getPartitionKey().size()];
        this.keyspace = tableMetadata.getKeyspace().getName();
    }

    @Override
    public String getQueryString() {
        maybeRebuildCache();
        return cache;
    }

    private void maybeRebuildCache() {
        if (!dirty && cache != null)
            return;

        StringBuilder sb;
        values = null;

        if (hasBindMarkers || forceNoValues) {
            sb = buildQueryString(null);
        } else {
            List<ByteBuffer> l = new ArrayList<ByteBuffer>();
            sb = buildQueryString(l);
            if (!l.isEmpty())
                values = l.toArray(new ByteBuffer[l.size()]);
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

    abstract StringBuilder buildQueryString(List<ByteBuffer> variables);

    boolean isCounterOp() {
        return isCounterOp == null ? false : isCounterOp;
    }

    void setCounterOp(boolean isCounterOp) {
        this.isCounterOp = isCounterOp;
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
        if (routingKey == null || name == null || value == null || value instanceof BindMarker)
            return;

        for (int i = 0; i < partitionKey.size(); i++) {
            if (name.equals(partitionKey.get(i).getName()) && Utils.isRawValue(value)) {
                routingKey[i] = partitionKey.get(i).getType().parse(Utils.toRawString(value));
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
        maybeRebuildCache();
        return values;
    }

    @Override
    public String toString() {
        if (forceNoValues)
            return getQueryString();

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
     * setForceNoValues(true)}, you are guarateed that {@code getValues()} will
     * return {@code null} and that the string returned by {@code
     * getQueryString()} will contain no other bind markers than the one
     * inputed by the user.
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

    // This is a duplicate of the one in SimpleStatement, but I don't want to expose this publicly so...
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

    /**
     * An utility class to create a BuiltStatement that encapsulate another one.
     */
    abstract static class ForwardingStatement<T extends BuiltStatement> extends BuiltStatement {

        T statement;

        ForwardingStatement(T statement) {
            super((String)null);
            this.statement = statement;
        }

        @Override
        public String getQueryString() {
            return statement.getQueryString();
        }

        @Override
        StringBuilder buildQueryString(List<ByteBuffer> values) {
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
