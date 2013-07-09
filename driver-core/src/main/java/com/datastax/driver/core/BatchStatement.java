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
 * A statement that group a number of {@link Statement} and
 * {@link BoundStatement} so they get executed as a batch.
 */
public class BatchStatement extends Query {

    private final List<Query> queries = new ArrayList<Query>();

    public BatchStatement() {}

    IdAndValues getIdAndValues() {
        IdAndValues idAndVals = new IdAndValues(queries.size());
        for (Query query : queries) {
            if (query instanceof Statement) {
                Statement st = (Statement)query;
                ByteBuffer[] vals = st.getValues();
                idAndVals.ids.add(st.getQueryString());
                idAndVals.values.add(vals == null ? Collections.<ByteBuffer>emptyList() : Arrays.asList(vals));
            } else if (query instanceof BoundStatement) {
                BoundStatement st = (BoundStatement)query;
                idAndVals.ids.add(st.statement.id);
                idAndVals.values.add(Arrays.asList(st.values));
            } else {
                assert query instanceof BatchStatement;
                BatchStatement st = (BatchStatement)query;
                IdAndValues other = st.getIdAndValues();
                idAndVals.ids.addAll(other.ids);
                idAndVals.values.addAll(other.values);
            }
        }
        return idAndVals;
    }

    /**
     * Adds a new query to this batch.
     *
     * @param query the new query to add.
     */
    public void add(Query query) {
        queries.add(query);
    }

    @Override
    public ByteBuffer getRoutingKey() {
        for (Query query : queries) {
            ByteBuffer rk = query.getRoutingKey();
            if (rk != null)
                return rk;
        }
        return null;
    }

    @Override
    public String getKeyspace() {
        for (Query query : queries) {
            String keyspace = query.getKeyspace();
            if (keyspace != null)
                return keyspace;
        }
        return null;
    }

    static class IdAndValues {

        public final List<Object> ids;
        public final List<List<ByteBuffer>> values;

        IdAndValues(int nbQueries) {
            ids = new ArrayList<Object>(nbQueries);
            values = new ArrayList<List<ByteBuffer>>(nbQueries);
        }
    }
}
