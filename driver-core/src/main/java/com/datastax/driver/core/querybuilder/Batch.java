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

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A built BATCH statement.
 */
public class Batch extends BuiltStatement {

    private final List<RegularStatement> statements;
    private final boolean logged;
    private final Options usings;

    // Only used when we add at last one statement that is not a BuiltStatement subclass
    private int nonBuiltStatementValues;

    Batch(RegularStatement[] statements, boolean logged) {
        super((String) null);
        this.statements = statements.length == 0
                ? new ArrayList<RegularStatement>()
                : new ArrayList<RegularStatement>(statements.length);
        this.logged = logged;
        this.usings = new Options(this);

        for (int i = 0; i < statements.length; i++)
            add(statements[i]);
    }

    @Override
    StringBuilder buildQueryString(List<Object> variables, CodecRegistry codecRegistry) {
        StringBuilder builder = new StringBuilder();

        builder.append(isCounterOp()
                ? "BEGIN COUNTER BATCH"
                : (logged ? "BEGIN BATCH" : "BEGIN UNLOGGED BATCH"));

        if (!usings.usings.isEmpty()) {
            builder.append(" USING ");
            Utils.joinAndAppend(builder, codecRegistry, " AND ", usings.usings, variables);
        }
        builder.append(' ');

        for (int i = 0; i < statements.size(); i++) {
            RegularStatement stmt = statements.get(i);
            if (stmt instanceof BuiltStatement) {
                BuiltStatement bst = (BuiltStatement) stmt;
                builder.append(maybeAddSemicolon(bst.buildQueryString(variables, codecRegistry)));

            } else {
                String str = stmt.getQueryString();
                builder.append(str);
                if (!str.trim().endsWith(";"))
                    builder.append(';');

                // Note that we force hasBindMarkers if there is any non-BuiltStatement, so we know
                // that we can only get there with variables == null
                assert variables == null;
            }
        }
        builder.append("APPLY BATCH;");
        return builder;
    }

    /**
     * Adds a new statement to this batch.
     *
     * @param statement the new statement to add.
     * @return this batch.
     * @throws IllegalArgumentException if counter and non-counter operations
     *                                  are mixed.
     */
    public Batch add(RegularStatement statement) {
        boolean isCounterOp = statement instanceof BuiltStatement && ((BuiltStatement) statement).isCounterOp();

        if (this.isCounterOp == null)
            setCounterOp(isCounterOp);
        else if (isCounterOp() != isCounterOp)
            throw new IllegalArgumentException("Cannot mix counter operations and non-counter operations in a batch statement");

        this.statements.add(statement);

        if (statement instanceof BuiltStatement) {
            this.hasBindMarkers |= ((BuiltStatement) statement).hasBindMarkers;
        } else {
            // For non-BuiltStatement, we cannot know if it includes a bind makers and we assume it does. In practice,
            // this means we will always serialize values as strings when there is non-BuiltStatement
            this.hasBindMarkers = true;
            this.nonBuiltStatementValues += ((SimpleStatement) statement).valuesCount();
        }

        checkForBindMarkers(null);

        return this;
    }

    @Override
    public ByteBuffer[] getValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        // If there is some non-BuiltStatement inside the batch with values, we shouldn't
        // use super.getValues() since it will ignore the values of said non-BuiltStatement.
        // If that's the case, we just collects all those values (and we know
        // super.getValues() == null in that case since we've explicitely set this.hasBindMarker
        // to true). Otherwise, we simply call super.getValues().
        if (nonBuiltStatementValues == 0)
            return super.getValues(protocolVersion, codecRegistry);

        ByteBuffer[] values = new ByteBuffer[nonBuiltStatementValues];
        int i = 0;
        for (RegularStatement statement : statements) {
            if (statement instanceof BuiltStatement)
                continue;

            ByteBuffer[] statementValues = statement.getValues(protocolVersion, codecRegistry);
            System.arraycopy(statementValues, 0, values, i, statementValues.length);
            i += statementValues.length;
        }
        return values;
    }

    /**
     * Adds a new options for this BATCH statement.
     *
     * @param using the option to add.
     * @return the options of this BATCH statement.
     */
    public Options using(Using using) {
        return usings.and(using);
    }

    /**
     * Returns the first non-null routing key of the statements in this batch
     * or null otherwise.
     *
     * @return the routing key for this batch statement.
     */
    @Override
    public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        for (RegularStatement statement : statements) {
            ByteBuffer routingKey = statement.getRoutingKey(protocolVersion, codecRegistry);
            if (routingKey != null) {
                return routingKey;
            }
        }
        return null;
    }

    /**
     * Returns the keyspace of the first statement in this batch.
     *
     * @return the keyspace of the first statement in this batch.
     */
    @Override
    public String getKeyspace() {
        return statements.isEmpty() ? null : statements.get(0).getKeyspace();
    }

    /**
     * The options of a BATCH statement.
     */
    public static class Options extends BuiltStatement.ForwardingStatement<Batch> {

        private final List<Using> usings = new ArrayList<Using>();

        Options(Batch statement) {
            super(statement);
        }

        /**
         * Adds the provided option.
         *
         * @param using a BATCH option.
         * @return this {@code Options} object.
         */
        public Options and(Using using) {
            usings.add(using);
            checkForBindMarkers(using);
            return this;
        }

        /**
         * Adds a new statement to the BATCH statement these options are part of.
         *
         * @param statement the statement to add.
         * @return the BATCH statement these options are part of.
         */
        public Batch add(RegularStatement statement) {
            return this.statement.add(statement);
        }
    }
}
