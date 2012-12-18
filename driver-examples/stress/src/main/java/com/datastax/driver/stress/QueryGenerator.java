package com.datastax.driver.stress;

import java.util.Iterator;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;

import joptsimple.OptionSet;

public abstract class QueryGenerator implements Iterator<QueryGenerator.Request> {

    static final Request DONE_MARKER = new Request() {
        public ResultSet execute(Session session) throws NoHostAvailableException { return null; }
        public ResultSetFuture executeAsync(Session session) throws NoHostAvailableException { return null; };
    };

    protected final int iterations;

    protected QueryGenerator(int iterations) {
        this.iterations = iterations;
    }

    public abstract void createSchema(Session session) throws NoHostAvailableException;

    public interface Builder {
        public QueryGenerator create(int iterations, OptionSet options);
    }

    public interface Request {

        public ResultSet execute(Session session) throws NoHostAvailableException;

        public ResultSetFuture executeAsync(Session session) throws NoHostAvailableException;

        public static class SimpleQuery implements Request {

            private final Query query;

            public SimpleQuery(Query query) {
                this.query = query;
            }

            public ResultSet execute(Session session) throws NoHostAvailableException {
                return session.execute(query);
            }

            public ResultSetFuture executeAsync(Session session) throws NoHostAvailableException {
                return session.executeAsync(query);
            }
        }

        public static class PreparedQuery implements Request {

            private final BoundStatement query;

            public PreparedQuery(BoundStatement query) {
                this.query = query;
            }

            public ResultSet execute(Session session) throws NoHostAvailableException {
                return session.execute(query);
            }

            public ResultSetFuture executeAsync(Session session) throws NoHostAvailableException {
                return session.executeAsync(query);
            }
        }
    }
}
