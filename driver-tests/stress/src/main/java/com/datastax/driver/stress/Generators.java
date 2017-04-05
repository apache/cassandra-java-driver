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
package com.datastax.driver.stress;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.utils.Bytes;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.nio.ByteBuffer;
import java.util.Random;

public class Generators {
    private static ThreadLocal<Random> random = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
            return new Random();
        }
    };

    private static ByteBuffer makeValue(final int valueSize) {
        byte[] value = new byte[valueSize];
        random.get().nextBytes(value);
        return ByteBuffer.wrap(value);
    }

    private static abstract class AbstractGenerator extends QueryGenerator {
        protected int iteration;

        protected AbstractGenerator(int iterations) {
            super(iterations);
        }

        @Override
        public int currentIteration() {
            return iteration;
        }

        @Override
        public boolean hasNext() {
            return iterations == -1 || iteration < iterations;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static final QueryGenerator.Builder INSERTER = new QueryGenerator.Builder() {

        @Override
        public String name() {
            return "insert";
        }

        @Override
        public OptionParser addOptions(OptionParser parser) {
            String msg = "Simple insertion of CQL3 rows (using prepared statements unless the --no-prepare option is used). "
                    + "The inserted rows have a fixed set of columns but no clustering columns.";
            parser.formatHelpWith(Stress.Help.formatFor(name(), msg));

            parser.accepts("no-prepare", "Do no use prepared statement");
            parser.accepts("columns-per-row", "Number of columns per CQL3 row").withRequiredArg().ofType(Integer.class).defaultsTo(5);
            parser.accepts("value-size", "The size in bytes for column values").withRequiredArg().ofType(Integer.class).defaultsTo(34);
            parser.accepts("with-compact-storage", "Use COMPACT STORAGE on the table used");
            return parser;
        }

        @Override
        public void prepare(OptionSet options, Session session) {

            try {
                session.execute("DROP KEYSPACE stress;");
            } catch (QueryValidationException e) { /* Fine, ignore */ }

            session.execute("CREATE KEYSPACE stress WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

            session.execute("USE stress");

            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE standard1 (key bigint PRIMARY KEY");
            for (int i = 0; i < (Integer) options.valueOf("columns-per-row"); ++i)
                sb.append(", C").append(i).append(" blob");
            sb.append(')');

            if (options.has("with-compact-storage"))
                sb.append(" WITH COMPACT STORAGE");

            session.execute(sb.toString());
        }

        @Override
        public QueryGenerator create(int id, int iterations, OptionSet options, Session session) {

            return options.has("no-prepare")
                    ? createRegular(id, iterations, options, session)
                    : createPrepared(id, iterations, options, session);
        }

        public QueryGenerator createRegular(int id, int iterations, OptionSet options, final Session session) {
            final int valueSize = (Integer) options.valueOf("value-size");
            final int columnsPerRow = (Integer) options.valueOf("columns-per-row");
            final long prefix = (long) id << 32;

            return new AbstractGenerator(iterations) {
                @Override
                public QueryGenerator.Request next() {
                    StringBuilder sb = new StringBuilder();
                    sb.append("UPDATE standard1 SET ");
                    for (int i = 0; i < columnsPerRow; ++i) {
                        if (i > 0) sb.append(", ");
                        sb.append('C').append(i).append("='").append(Bytes.toHexString(makeValue(valueSize))).append('\'');
                    }
                    sb.append(" WHERE key = ").append(prefix | iteration);
                    ++iteration;
                    return new QueryGenerator.Request.SimpleQuery(new SimpleStatement(sb.toString()));
                }
            };
        }

        public QueryGenerator createPrepared(int id, int iterations, OptionSet options, Session session) {
            final int valueSize = (Integer) options.valueOf("value-size");
            final int columnsPerRow = (Integer) options.valueOf("columns-per-row");
            final long prefix = (long) id << 32;

            StringBuilder sb = new StringBuilder();
            sb.append("UPDATE standard1 SET ");
            for (int i = 0; i < columnsPerRow; ++i) {
                if (i > 0) sb.append(", ");
                sb.append('C').append(i).append("=?");
            }
            sb.append(" WHERE key = ?");

            final PreparedStatement stmt = session.prepare(sb.toString());

            return new AbstractGenerator(iterations) {
                @Override
                public QueryGenerator.Request next() {
                    BoundStatement b = stmt.bind();
                    b.setLong("key", prefix | iteration);
                    for (int i = 0; i < columnsPerRow; ++i)
                        b.setBytes("c" + i, makeValue(valueSize));
                    ++iteration;
                    return new QueryGenerator.Request.PreparedQuery(b);
                }
            };
        }
    };

    public static final QueryGenerator.Builder READER = new QueryGenerator.Builder() {

        @Override
        public String name() {
            return "read";
        }

        @Override
        public OptionParser addOptions(OptionParser parser) {
            String msg = "Read the rows inserted with the insert generator. Use prepared statements unless the --no-prepare option is used.";
            parser.formatHelpWith(Stress.Help.formatFor(name(), msg));

            parser.accepts("no-prepare", "Do no use prepared statement");
            return parser;
        }

        @Override
        public void prepare(OptionSet options, Session session) {
            KeyspaceMetadata ks = session.getCluster().getMetadata().getKeyspace("stress");
            if (ks == null || ks.getTable("standard1") == null) {
                System.err.println("There is nothing to reads, please run insert/insert_prepared first.");
                System.exit(1);
            }

            session.execute("USE stress");
        }

        @Override
        public QueryGenerator create(int id, int iterations, OptionSet options, Session session) {
            return options.has("no-prepare")
                    ? createRegular(id, iterations, session)
                    : createPrepared(id, iterations, session);
        }

        public QueryGenerator createRegular(long id, int iterations, final Session session) {
            final long prefix = (long) id << 32;
            return new AbstractGenerator(iterations) {
                @Override
                public QueryGenerator.Request next() {
                    StringBuilder sb = new StringBuilder();
                    sb.append("SELECT * FROM standard1 WHERE key = ").append(prefix | iteration);
                    ++iteration;
                    return new QueryGenerator.Request.SimpleQuery(new SimpleStatement(sb.toString()));
                }
            };
        }

        public QueryGenerator createPrepared(long id, int iterations, Session session) {
            final long prefix = (long) id << 32;
            final PreparedStatement stmt = session.prepare("SELECT * FROM standard1 WHERE key = ?");

            return new AbstractGenerator(iterations) {
                @Override
                public QueryGenerator.Request next() {
                    BoundStatement bs = stmt.bind();
                    bs.setLong("key", prefix | iteration);
                    ++iteration;
                    return new QueryGenerator.Request.PreparedQuery(bs);
                }
            };
        }
    };
}
