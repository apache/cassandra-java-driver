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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;

/**
 * A built BATCH statement.
 */
public class Batch extends BuiltStatement {

    private final List<RegularStatement> statements;
    private final boolean logged;
    private final Options usings;

    Batch(ProtocolVersion protocolVersion, CodecRegistry codecRegistry, RegularStatement[] statements, boolean logged) {
        super((String)null, protocolVersion, codecRegistry);
        this.statements = statements.length == 0
                        ? new ArrayList<RegularStatement>()
                        : new ArrayList<RegularStatement>(statements.length);
        this.logged = logged;
        this.usings = new Options(this);

        for (RegularStatement statement : statements)
            add(statement);
    }

    @Override
    StringBuilder buildQueryString(List<Object> variables) {
        StringBuilder builder = new StringBuilder();

        builder.append(isCounterOp()
                       ? "BEGIN COUNTER BATCH"
                       : (logged ? "BEGIN BATCH" : "BEGIN UNLOGGED BATCH"));

        if (!usings.usings.isEmpty()) {
            builder.append(" USING ");
            Utils.joinAndAppend(builder, codecRegistry, " AND ", usings.usings, variables);
        }
        builder.append(' ');

        for (RegularStatement stmt : statements) {
            if (stmt instanceof BuiltStatement) {
                BuiltStatement bst = (BuiltStatement)stmt;
                builder.append(maybeAddSemicolon(bst.buildQueryString(variables)));
            } else {
                String str = stmt.getQueryString();
                builder.append(str);
                if (!str.trim().endsWith(";"))
                    builder.append(';');

                // If there are non-built child statements, we're not collecting values.
                // All variables in built statements will be inlined in the query string, but
                // for non-built statements we need to copy the values to the parent batch.
                assert variables == null;
                this.copyValues(stmt);
            }
            builder.append(' ');
        }
        builder.append("APPLY BATCH;");
        return builder;
    }

    /**
     * Adds a new statement to this batch.
     *
     * @param statement the new statement to add.
     * @return this batch.
     *
     * @throws IllegalArgumentException if counter and non-counter operations
     * are mixed.
     */
    public Batch add(RegularStatement statement) {
        // We can't handle collisions if multiple statement use the same names, so avoid names altogether
        Preconditions.checkArgument(!statement.usesNamedValues(),
            "Statements with named values are not supported in built batches, use positional values instead");

        boolean isCounterOp = statement instanceof BuiltStatement && ((BuiltStatement) statement).isCounterOp();

        if (this.isCounterOp == null)
            setCounterOp(isCounterOp);
        else if (isCounterOp() != isCounterOp)
            throw new IllegalArgumentException("Cannot mix counter operations and non-counter operations in a batch statement");

        this.statements.add(statement);

        if (statement instanceof BuiltStatement) {
            BuiltStatement builtStatement = (BuiltStatement)statement;
            this.hasBindMarkers |= builtStatement.hasBindMarkers;
        } else {
            // For non-BuiltStatement, we cannot know if they include bind markers,
            // so we assume they do.
            // If that's the case, the user meant the whole statement to be prepared,
            // and we shouldn't add our own markers.
            // In practice, this means that the batch statement will never add its own
            // bind markers as soon as at least one of its components is assumed to contain those.
            this.hasBindMarkers = true;
        }

        checkForBindMarkers(null);

        return this;
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
    public ByteBuffer getRoutingKey() {
        for (RegularStatement statement : statements) {
            ByteBuffer routingKey = statement.getRoutingKey();
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
