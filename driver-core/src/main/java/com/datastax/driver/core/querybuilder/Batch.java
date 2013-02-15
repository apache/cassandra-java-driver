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

import com.datastax.driver.core.Statement;

/**
 * A built BATCH statement.
 */
public class Batch extends BuiltStatement {

    private final List<Statement> statements;
    private final Options usings;
    private ByteBuffer routingKey;

    Batch(Statement[] statements) {
        this.statements = statements.length == 0
                        ? new ArrayList<Statement>()
                        : new ArrayList<Statement>(statements.length);
        this.usings = new Options(this);

        for (int i = 0; i < statements.length; i++)
            add(statements[i]);
    }

    protected String buildQueryString() {
        StringBuilder builder = new StringBuilder();
        builder.append("BEGIN BATCH");

        if (!usings.usings.isEmpty()) {
            builder.append(" USING ");
            Utils.joinAndAppend(builder, " AND ", usings.usings);
        }
        builder.append(" ");

        for (int i = 0; i < statements.size(); i++) {
            String str = statements.get(i).getQueryString();
            builder.append(str);
            if (!str.trim().endsWith(";"))
                builder.append(";");
        }
        builder.append("APPLY BATCH;");
        return builder.toString();
    }

    /**
     * Adds a new statement to this batch.
     *
     * @param statement the new statement to add.
     * @return this batch.
     */
    public Batch add(Statement statement) {
        this.statements.add(statement);
        setDirty();

        if (routingKey == null && statement.getRoutingKey() != null)
            routingKey = statement.getRoutingKey();

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
        return routingKey;
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
            setDirty();
            return this;
        }

        /**
         * Adds a new statement to the BATCH statement these options are part of.
         *
         * @param statement the statement to add.
         * @return the BATCH statement these options are part of.
         */
        public Batch add(Statement statement) {
            return this.statement.add(statement);
        }
    }
}
