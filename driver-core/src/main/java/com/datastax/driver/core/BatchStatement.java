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
import java.util.Collections;
import java.util.List;

/**
 * A statement that group a number of {@link Statement} so they get executed as
 * a batch.
 */
public class BatchStatement extends Statement {

    private final List<Statement> statements = new ArrayList<Statement>();

    public BatchStatement() {}

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
                idAndVals.ids.add(st.statement.id);
                idAndVals.values.add(Arrays.asList(st.values));
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
     *
     * @param statement the new statement to add.
     * @return this batch statement.
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
            statements.add(statement);
        }
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
