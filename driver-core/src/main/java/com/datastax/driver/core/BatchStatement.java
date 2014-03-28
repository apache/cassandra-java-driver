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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * A statement that group a number of {@link Statement} so they get executed as
 * a batch.
 * <p>
 * Note: BatchStatement is not supported with the native protocol version 1: you
 * will get an {@link UnsupportedProtocolVersionException} when submitting one if
 * version 1 of the protocol is in use (i.e. if you've force version 1 through
 * {@link Cluster.Builder#withProtocolVersion} or you use Cassandra 1.2). Note
 * however that you can still use <a href="http://cassandra.apache.org/doc/cql3/CQL.html#batchStmt">CQL Batch statements</a>
 * even with the protocol version 1.
 */
public class BatchStatement extends Statement {

    /**
     * The type of batch to use.
     */
    public enum Type {
        /**
         * A logged batch: Cassandra will first the batch to its distributed batch log to
         * ensure the atomicity of the batch.
         */
        LOGGED,

        /**
         * A batch that doesn't use Cassandra's distributed batch log. Such batch are not
         * guaranteed to be atomic.
         */
        UNLOGGED,

        /**
         * A counter batch. Note that such batch is the only type that can contain counter
         * operations and it can only contain these.
         */
        COUNTER
    };

    final Type batchType;
    private final List<Statement> statements = new ArrayList<Statement>();

    /**
     * Creates a new {@code LOGGED} batch statement.
     */
    public BatchStatement() {
        this(Type.LOGGED);
    }

    /**
     * Creates a new batch statement of the provided type.
     *
     * @param batchType the type of batch.
     */
    public BatchStatement(Type batchType) {
        this.batchType = batchType;
    }

    IdAndValues getIdAndValues() {
        IdAndValues idAndVals = new IdAndValues(statements.size());
        for (Statement statement : statements) {
            if (statement instanceof RegularStatement) {
                RegularStatement st = (RegularStatement)statement;
                ByteBuffer[] vals = st.getValues();
                idAndVals.ids.add(st.getQueryString());
                idAndVals.values.add(vals == null ? Collections.<ByteBuffer>emptyList() : Arrays.asList(vals));
            } else {
                // We handle BatchStatement in add() so ...
                assert statement instanceof BoundStatement;
                BoundStatement st = (BoundStatement)statement;
                idAndVals.ids.add(st.statement.getPreparedId().id);
                idAndVals.values.add(Arrays.asList(st.wrapper.values));
            }
        }
        return idAndVals;
    }

    /**
     * Adds a new statement to this batch.
     * <p>
     * Note that {@code statement} can be any {@code Statement}. It is allowed to mix
     * {@code RegularStatement} and {@code BoundStatement} in the same
     * {@code BatchStatement} in particular. Adding another {@code BatchStatement}
     * is also allowed for convenient and is equivalent to adding all the {@code Statement}
     * contained in that other {@code BatchStatement}.
     * <p>
     * Please note that the options of the added Statement (all those defined directly by the
     * {@link Statement} class: consistency level, fetch size, tracing, ...) will be ignored
     * for the purpose of the execution of the Batch. Instead, the options used are the one
     * of this {@code BatchStatement} object.
     *
     * @param statement the new statement to add.
     * @return this batch statement.
     *
     * @throws IllegalStateException if adding the new statement means that this
     * {@code BatchStatement} has more than 65536 statements (since this is the maximum number
     * of statements for a BatchStatement allowed by the underlying protocol).
     */
    public BatchStatement add(Statement statement) {

        // We handle BatchStatement here (rather than in getIdAndValues) as it make it slightly
        // easier to avoid endless loop if the use mistakenly pass a batch that depends on this
        // object (or this directly).
        if (statement instanceof BatchStatement) {
            for (Statement subStatements : ((BatchStatement)statement).statements) {
                add(subStatements);
            }
        } else {
            if (statements.size() >= 0xFFFF)
                throw new IllegalStateException("Batch statement cannot contain more than " + 0xFFFF + " statements.");
            statements.add(statement);
        }
        return this;
    }

    /**
     * Adds multiple statements to this batch.
     * <p>
     * This is a shortcut method that calls {@link #add} on all the statements
     * from {@code statements}.
     *
     * @param statements the statements to add.
     * @return this batch statement.
     */
    public BatchStatement addAll(Iterable<? extends Statement> statements) {
        for (Statement statement : statements)
            add(statement);
        return this;
    }

    /**
     * The statements that have been added to this batch so far.
     *
     * @return an (immutable) collection of the statements that have been added
     * to this batch so far.
     */
    public Collection<Statement> getStatements() {
        return ImmutableList.copyOf(statements);
    }

    /**
     * Clears this batch, removing all statements added so far.
     *
     * @return this (now empty) {@code BatchStatement}.
     */
    public BatchStatement clear() {
        statements.clear();
        return this;
    }

    @Override
    public ByteBuffer getRoutingKey() {
        for (Statement statement : statements) {
            ByteBuffer rk = statement.getRoutingKey();
            if (rk != null)
                return rk;
        }
        return null;
    }

    @Override
    public String getKeyspace() {
        for (Statement statement : statements) {
            String keyspace = statement.getKeyspace();
            if (keyspace != null)
                return keyspace;
        }
        return null;
    }

    static class IdAndValues {

        public final List<Object> ids;
        public final List<List<ByteBuffer>> values;

        IdAndValues(int nbstatements) {
            ids = new ArrayList<Object>(nbstatements);
            values = new ArrayList<List<ByteBuffer>>(nbstatements);
        }
    }
}
