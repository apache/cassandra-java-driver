package com.datastax.driver.examples.stress;

import java.util.Iterator;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;

public abstract class QueryGenerator implements Iterator<QueryGenerator.Request> {

    static final Request DONE_MARKER = new Request() {
        public ResultSet execute(Session session) throws NoHostAvailableException { return null; }
        public ResultSet.Future executeAsync(Session session) throws NoHostAvailableException { return null; };
    };

    protected final int iterations;

    protected QueryGenerator(int iterations) {
        this.iterations = iterations;
    }

    public abstract void createSchema(Session session) throws NoHostAvailableException;

    public interface Builder {
        public QueryGenerator create(int iterations);
    }

    public interface Request {

        public ResultSet execute(Session session) throws NoHostAvailableException;

        public ResultSet.Future executeAsync(Session session) throws NoHostAvailableException;

        public static class SimpleQuery implements Request {

            private final String query;
            private final QueryOptions options;

            public SimpleQuery(String query, QueryOptions options) {
                this.query = query;
                this.options = options;
            }

            public ResultSet execute(Session session) throws NoHostAvailableException {
                return session.execute(query, options);
            }

            public ResultSet.Future executeAsync(Session session) throws NoHostAvailableException {
                return session.executeAsync(query, options);
            }
        }

        public static class PreparedQuery implements Request {

            private final BoundStatement query;
            private final QueryOptions options;

            public PreparedQuery(BoundStatement query, QueryOptions options) {
                this.query = query;
                this.options = options;
            }

            public ResultSet execute(Session session) throws NoHostAvailableException {
                return session.executePrepared(query, options);
            }

            public ResultSet.Future executeAsync(Session session) throws NoHostAvailableException {
                return session.executePreparedAsync(query, options);
            }
        }
    }
}
