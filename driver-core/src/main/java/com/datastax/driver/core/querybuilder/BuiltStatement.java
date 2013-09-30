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
import java.util.List;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.RetryPolicy;

abstract class BuiltStatement extends Statement {

    private final List<ColumnMetadata> partitionKey;
    private final ByteBuffer[] routingKey;
    private boolean dirty;
    private String cache;
    protected Boolean isCounterOp;

    protected BuiltStatement() {
        this.partitionKey = null;
        this.routingKey = null;
    }

    protected BuiltStatement(TableMetadata tableMetadata) {
        this.partitionKey = tableMetadata.getPartitionKey();
        this.routingKey = new ByteBuffer[tableMetadata.getPartitionKey().size()];
    }

    @Override
    public String getQueryString() {
        if (dirty || cache == null) {
            StringBuilder sb = buildQueryString();

            // Use the same test that String#trim() uses to determine
            // if a character is a whitespace character.
            int l = sb.length();
            while (l > 0 && sb.charAt(l - 1) <= ' ')
                l -= 1;
            if (l != sb.length())
                sb.setLength(l);

            if (l == 0 || sb.charAt(l - 1) != ';')
                sb.append(';');

            cache = sb.toString();
            dirty = false;
        }
        return cache;
    }

    protected abstract StringBuilder buildQueryString();

    protected void setDirty() {
        dirty = true;
    }

    protected boolean isCounterOp() {
        return isCounterOp == null ? false : isCounterOp;
    }

    protected void setCounterOp(boolean isCounterOp) {
        this.isCounterOp = isCounterOp;
    }

    // TODO: Correctly document the InvalidTypeException
    void maybeAddRoutingKey(String name, Object value) {
        if (routingKey == null || name == null || value == null || value == QueryBuilder.BIND_MARKER)
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

        protected T statement;

        protected ForwardingStatement(T statement) {
            this.statement = statement;
        }

        @Override
        public String getQueryString() {
            return statement.getQueryString();
        }

        @Override
        protected StringBuilder buildQueryString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer getRoutingKey() {
            return statement.getRoutingKey();
        }

        @Override
        protected void setDirty() {
            statement.setDirty();
        }

        @Override
        protected boolean isCounterOp() {
            return statement.isCounterOp();
        }

        @Override
        public Query setConsistencyLevel(ConsistencyLevel consistency) {
            statement.setConsistencyLevel(consistency);
            return this;
        }

        @Override
        public ConsistencyLevel getConsistencyLevel() {
            return statement.getConsistencyLevel();
        }

        @Override
        public Query enableTracing() {
            statement.enableTracing();
            return this;
        }

        @Override
        public Query disableTracing() {
            statement.disableTracing();
            return this;
        }

        @Override
        public boolean isTracing() {
            return statement.isTracing();
        }

        @Override
        public Query setRetryPolicy(RetryPolicy policy) {
            statement.setRetryPolicy(policy);
            return this;
        }

        @Override
        public RetryPolicy getRetryPolicy() {
            return statement.getRetryPolicy();
        }
    }
}
