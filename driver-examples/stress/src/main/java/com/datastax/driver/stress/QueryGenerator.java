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

import java.util.Iterator;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;

import joptsimple.OptionSet;

public abstract class QueryGenerator implements Iterator<QueryGenerator.Request> {

    static final Request DONE_MARKER = new Request() {
        public ResultSet execute(Session session) { return null; }
        public ResultSetFuture executeAsync(Session session) { return null; };
    };

    protected final int iterations;

    protected QueryGenerator(int iterations) {
        this.iterations = iterations;
    }

    public abstract void createSchema(Session session);

    public interface Builder {
        public QueryGenerator create(int iterations, OptionSet options);
    }

    public interface Request {

        public ResultSet execute(Session session);

        public ResultSetFuture executeAsync(Session session);

        public static class SimpleQuery implements Request {

            private final Query query;

            public SimpleQuery(Query query) {
                this.query = query;
            }

            public ResultSet execute(Session session) {
                return session.execute(query);
            }

            public ResultSetFuture executeAsync(Session session) {
                return session.executeAsync(query);
            }
        }

        public static class PreparedQuery implements Request {

            private final BoundStatement query;

            public PreparedQuery(BoundStatement query) {
                this.query = query;
            }

            public ResultSet execute(Session session) {
                return session.execute(query);
            }

            public ResultSetFuture executeAsync(Session session) {
                return session.executeAsync(query);
            }
        }
    }
}
