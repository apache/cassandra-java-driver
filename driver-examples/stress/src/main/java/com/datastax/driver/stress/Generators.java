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
package com.datastax.driver.stress;

import java.nio.ByteBuffer;
import java.util.Random;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;

import joptsimple.OptionSet;

import org.apache.cassandra.utils.ByteBufferUtil;

public class Generators {

    private static ThreadLocal<Random> random = new ThreadLocal<Random>() {
        protected Random initialValue() {
            return new Random();
        }
    };

    private static void createCassandraStressTables(Session session, OptionSet options) {
        try {
            session.execute("CREATE KEYSPACE stress WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        } catch (AlreadyExistsException e) { /* It's ok, ignore */ }

        session.execute("USE stress");

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE Standard1 (key bigint PRIMARY KEY");
        for (int i = 0; i < (Integer)options.valueOf("columns-per-row"); ++i)
            sb.append(", C").append(i).append(" blob");
        sb.append(")");

        try {
            session.execute(sb.toString());
        } catch (AlreadyExistsException e) { /* It's ok, ignore */ }
    }

    private static ByteBuffer makeValue(final int valueSize) {
        byte[] value = new byte[valueSize];
        random.get().nextBytes(value);
        return ByteBuffer.wrap(value);
    }

    public static final QueryGenerator.Builder CASSANDRA_INSERTER = new QueryGenerator.Builder() {

        public void createSchema(OptionSet options, Session session) {
            createCassandraStressTables(session, options);
        }

        public QueryGenerator create(final int id,
                                     final int iterations,
                                     final OptionSet options,
                                     final Session session) {

            final int valueSize = (Integer)options.valueOf("value-size");
            final int columnsPerRow = (Integer)options.valueOf("columns-per-row");
            final long prefix = (long) id << 32;

            return new QueryGenerator(iterations) {
                private int i;

                public boolean hasNext() {
                    return iterations == -1 || i < iterations;
                }

                public QueryGenerator.Request next() {
                    StringBuilder sb = new StringBuilder();
                    sb.append("UPDATE Standard1 SET ");
                    for (int i = 0; i < columnsPerRow; ++i) {
                        if (i > 0) sb.append(", ");
                        sb.append("C").append(i).append("='").append(ByteBufferUtil.bytesToHex(makeValue(valueSize))).append("'");
                    }
                    sb.append(" WHERE key = ").append(prefix | i);
                    ++i;
                    return new QueryGenerator.Request.SimpleQuery(new SimpleStatement(sb.toString()));
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    };

    public static final QueryGenerator.Builder CASSANDRA_PREPARED_INSERTER = new QueryGenerator.Builder() {

        public void createSchema(OptionSet options, Session session) {
            createCassandraStressTables(session, options);

        }

        public QueryGenerator create(final int id,
                                     final int iterations,
                                     final OptionSet options,
                                     final Session session) {

            final int columnsPerRow = (Integer)options.valueOf("columns-per-row");
            final int valueSize = (Integer)options.valueOf("value-size");
            final long prefix = (long) id << 32;

            StringBuilder sb = new StringBuilder();
            sb.append("UPDATE Standard1 SET ");
            for (int i = 0; i < columnsPerRow; ++i) {
                if (i > 0) sb.append(", ");
                sb.append("C").append(i).append("=?");
            }
            sb.append(" WHERE key = ?");

            final PreparedStatement stmt = session.prepare(sb.toString());

            return new QueryGenerator(iterations) {
                private int i;

                public boolean hasNext() {
                    return iterations == -1 || i < iterations;
                }

                public QueryGenerator.Request next() {
                    BoundStatement b = stmt.bind();
                    b.setLong("key", prefix | i);
                    for (int i = 0; i < columnsPerRow; ++i)
                        b.setBytes("c" + i, makeValue(valueSize));
                    ++i;
                    return new QueryGenerator.Request.PreparedQuery(b);
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    };
}
