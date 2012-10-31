package com.datastax.driver.examples.stress;

import com.datastax.driver.core.*;
import com.datastax.driver.core.configuration.*;
import com.datastax.driver.core.exceptions.*;

public class Generators {

    public static final QueryGenerator.Builder SIMPLE_INSERTER = new QueryGenerator.Builder() {
        public QueryGenerator create(final int iterations) {
            return new QueryGenerator(iterations) {
                private int i;

                public void createSchema(Session session) throws NoHostAvailableException {
                    try { session.execute("CREATE KEYSPACE stress_ks WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"); } catch (AlreadyExistsException e) { /* It's ok, ignore */ }
                    session.execute("USE stress_ks");

                    try {
                        session.execute("CREATE TABLE stress_cf (k int, c int, v int, PRIMARY KEY (k, c))");
                    } catch (AlreadyExistsException e) { /* It's ok, ignore */ }
                }

                public boolean hasNext() {
                    return i < iterations;
                }

                public QueryGenerator.Request next() {
                    String query = String.format("INSERT INTO stress_cf(k, c, v) VALUES (%d, %d, %d)", i, i, i);
                    ++i;
                    return new QueryGenerator.Request.SimpleQuery(query, new QueryOptions());
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    };

    public static final QueryGenerator.Builder SIMPLE_PREPARED_INSERTER = new QueryGenerator.Builder() {
        public QueryGenerator create(final int iterations) {
            return new QueryGenerator(iterations) {
                private int i;
                private PreparedStatement stmt;

                public void createSchema(Session session) throws NoHostAvailableException {
                    try { session.execute("CREATE KEYSPACE stress_ks WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"); } catch (AlreadyExistsException e) { /* It's ok, ignore */ }
                    session.execute("USE stress_ks");

                    try {
                        session.execute("CREATE TABLE stress_cf (k int, c int, v int, PRIMARY KEY (k, c))");
                    } catch (AlreadyExistsException e) { /* It's ok, ignore */ }

                    stmt = session.prepare("INSERT INTO stress_cf(k, c, v) VALUES (?, ?, ?)");
                }

                public boolean hasNext() {
                    return i < iterations;
                }

                public QueryGenerator.Request next() {
                    BoundStatement b = stmt.bind(i, i, i);
                    ++i;
                    return new QueryGenerator.Request.PreparedQuery(b, new QueryOptions());
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    };
}
