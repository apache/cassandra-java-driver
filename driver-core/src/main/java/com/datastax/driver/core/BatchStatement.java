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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A statement that groups a number of {@link Statement} so they get executed as
 * a batch.
 * <p/>
 * Note: BatchStatement is not supported with the native protocol version 1: you
 * will get an {@link UnsupportedFeatureException} when submitting one if
 * version 1 of the protocol is in use (i.e. if you've force version 1 through
 * {@link Cluster.Builder#withProtocolVersion} or you use Cassandra 1.2). Note
 * however that you can still use <a href="http://cassandra.apache.org/doc/cql3/CQL.html#batchStmt">CQL Batch statements</a>
 * even with the protocol version 1.
 * <p/>
 * Setting a BatchStatement's serial consistency level is only supported with the
 * native protocol version 3 or higher (see {@link #setSerialConsistencyLevel(ConsistencyLevel)}).
 */
public class BatchStatement extends Statement {

    /**
     * The type of batch to use.
     */
    public enum Type {
        /**
         * A logged batch: Cassandra will first write the batch to its distributed batch log
         * to ensure the atomicity of the batch (atomicity meaning that if any statement in
         * the batch succeeds, all will eventually succeed).
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
    }

    ;

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

    IdAndValues getIdAndValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        IdAndValues idAndVals = new IdAndValues(statements.size());
        for (Statement statement : statements) {
            if (statement instanceof StatementWrapper)
                statement = ((StatementWrapper) statement).getWrappedStatement();
            if (statement instanceof RegularStatement) {
                RegularStatement st = (RegularStatement) statement;
                ByteBuffer[] vals = st.getValues(protocolVersion, codecRegistry);
                String query = st.getQueryString(codecRegistry);
                idAndVals.ids.add(query);
                idAndVals.values.add(vals == null ? Collections.<ByteBuffer>emptyList() : Arrays.asList(vals));
            } else {
                // We handle BatchStatement in add() so ...
                assert statement instanceof BoundStatement;
                BoundStatement st = (BoundStatement) statement;
                idAndVals.ids.add(st.statement.getPreparedId().id);
                idAndVals.values.add(Arrays.asList(st.wrapper.values));
            }
        }
        return idAndVals;
    }

    /**
     * Adds a new statement to this batch.
     * <p/>
     * Note that {@code statement} can be any {@code Statement}. It is allowed to mix
     * {@code RegularStatement} and {@code BoundStatement} in the same
     * {@code BatchStatement} in particular. Adding another {@code BatchStatement}
     * is also allowed for convenience and is equivalent to adding all the {@code Statement}
     * contained in that other {@code BatchStatement}.
     * <p/>
     * Due to a protocol-level limitation, adding a {@code RegularStatement} with named values
     * is currently not supported; an {@code IllegalArgument} will be thrown.
     * <p/>
     * When adding a {@code BoundStatement}, all of its values must be set, otherwise an
     * {@code IllegalStateException} will be thrown when submitting the batch statement.
     * See {@link BoundStatement} for more details, in particular how to handle {@code null}
     * values.
     * <p/>
     * Please note that the options of the added Statement (all those defined directly by the
     * {@link Statement} class: consistency level, fetch size, tracing, ...) will be ignored
     * for the purpose of the execution of the Batch. Instead, the options used are the one
     * of this {@code BatchStatement} object.
     *
     * @param statement the new statement to add.
     * @return this batch statement.
     * @throws IllegalStateException    if adding the new statement means that this
     *                                  {@code BatchStatement} has more than 65536 statements (since this is the maximum number
     *                                  of statements for a BatchStatement allowed by the underlying protocol).
     * @throws IllegalArgumentException if adding a regular statement that uses named values.
     */
    public BatchStatement add(Statement statement) {
        if (statement instanceof StatementWrapper) {
            statement = ((StatementWrapper) statement).getWrappedStatement();
        }
        if ((statement instanceof RegularStatement) && ((RegularStatement) statement).usesNamedValues()) {
            throw new IllegalArgumentException("Batch statement cannot contain regular statements with named values ("
                    + ((RegularStatement) statement).getQueryString() + ")");
        }

        // We handle BatchStatement here (rather than in getIdAndValues) as it make it slightly
        // easier to avoid endless loops if the user mistakenly passes a batch that depends on this
        // object (or this directly).
        if (statement instanceof BatchStatement) {
            for (Statement subStatements : ((BatchStatement) statement).statements) {
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
     * <p/>
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

    /**
     * Returns the number of elements in this batch.
     *
     * @return the number of elements in this batch.
     */
    public int size() {
        return statements.size();
    }

    /**
     * Sets the serial consistency level for the query.
     * <p/>
     * This is only supported with version 3 or higher of the native protocol. If you call
     * this method when version 2 is in use, you will get an {@link UnsupportedFeatureException}
     * when submitting the statement. With version 2, protocol batches with conditions
     * have their serial consistency level hardcoded to SERIAL; if you need to execute a batch
     * with LOCAL_SERIAL, you will have to use a CQL batch.
     *
     * @param serialConsistency the serial consistency level to set.
     * @return this {@code Statement} object.
     * @throws IllegalArgumentException if {@code serialConsistency} is not one of
     *                                  {@code ConsistencyLevel.SERIAL} or {@code ConsistencyLevel.LOCAL_SERIAL}.
     * @see Statement#setSerialConsistencyLevel(ConsistencyLevel)
     */
    @Override
    public BatchStatement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
        return (BatchStatement) super.setSerialConsistencyLevel(serialConsistency);
    }

    @Override
    public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        for (Statement statement : statements) {
            if (statement instanceof StatementWrapper)
                statement = ((StatementWrapper) statement).getWrappedStatement();
            ByteBuffer rk = statement.getRoutingKey(protocolVersion, codecRegistry);
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

    @Override
    public Boolean isIdempotent() {
        if (idempotent != null) {
            return idempotent;
        }
        return isBatchIdempotent(statements);
    }

    void ensureAllSet() {
        for (Statement statement : statements)
            if (statement instanceof BoundStatement)
                ((BoundStatement) statement).ensureAllSet();
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
